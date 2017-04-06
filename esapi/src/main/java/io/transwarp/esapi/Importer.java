package io.transwarp.esapi;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by 杨发林 on 2016/11/22.
 */


public class Importer {

    /*
    打印日志
     */
    private static final Log log = LogFactory.getLog(Importer.class);
    /*
      ElasticSearch 单播配置下，节点向指定的主机发送单播请求，配置如下：
     */
    static final String UNICAST_HOSTS_KEY = "discovery.zen.ping.unicast.hosts";
    /*
    TDH　Cluster Name
     */
    static final String CLUSTER_NAME_KEY = "cluster.name";
    /*
    concurrent thread number
     */
    static final String BULK_IMPORT_CONCURRENT_KEY = "bulk.import.concurrent";
    /*
    batch size
     */
    static final String BULK_IMPORT_BATCHSIZE_KEY = "bulk.import.batchsize";
   /*
   文件的位置，现在只有hdfs和本地的文件可以使用这个代码，其他的数据源需要单独实现producer
    */
    static final String BULK_IMPORT_FSTYPE_KEY = "bulk.import.fstype"; // hdfs or local
    /*
    指定源数据的压缩格式gz或者null，有其余的压缩格式需要修改代码
     */
    static final String BULK_IMPORT_COMPRESSION_KEY = "bulk.import.compression"; // gz or null
    /*
    creator class name 需要全路径例如 io.transwarp.esapi.ImpleCreator
     */
    static final String BULK_IMPORT_CREATECLASS_KEY = "bulk.import.creator";
    /*
    index name
     */
    static final String BULK_IMPORT_INDEX_NAME_KEY = "bulk.import.index.name";
    /*
    尝试的次数
     */
    static final int maxRetryTime = 10;
    /*
    等待时间
     */
    static final int baseWaitTime = 100; // 100 ms
    /*
    延迟时间
     */
    static final int maxDelayFactor = 16; // 1600 ms
    /*
     线程工厂对象，负者创建新的工作线程
     */
    private static ThreadFactory threadFactory = new ThreadFactory() {

        private ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        private AtomicLong threadCounter = new AtomicLong(0L);

        public Thread newThread(Runnable r) {
            Thread thread = defaultThreadFactory.newThread(r);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    log.error("uncaught exception: " + t.getName() + " " + t.getId(), e);
                }
            });
            thread.setName("BulkImportWorker-" + threadCounter.incrementAndGet());
            return thread;
        }
    };

    /*
    main 函数，程序的入口，完成数据的导入和创建索引
     */
    public static void main(String args[]) throws Exception {

        int concurrent = Integer.parseInt(System.getProperty(BULK_IMPORT_CONCURRENT_KEY,
                String.valueOf(Runtime.getRuntime().availableProcessors())));

        int batchSize = Integer.parseInt(System.getProperty(BULK_IMPORT_BATCHSIZE_KEY, "500"));

        Client client = buildEsClient();
        Producer producer = buildProducer();

        String creator = System.getProperty(BULK_IMPORT_CREATECLASS_KEY);
        if (creator == null) {
            throw new RuntimeException("missing config " + BULK_IMPORT_CREATECLASS_KEY);
        }

        String indexName = System.getProperty(BULK_IMPORT_INDEX_NAME_KEY);
        if (indexName == null) {
            throw new RuntimeException("missing config " + BULK_IMPORT_INDEX_NAME_KEY);
        }

        mayCreateIndex(client, indexName, creator);

        load(new LoadArg(client, producer, concurrent, batchSize, creator));
    }
    /*
     File System Type HDFS or LOCAL
     */
    private enum FSTYPE {
        HDFS,
        LOCAL
    }
    /*
    COMPRESSION TYPE GZ or NONE
     */
    private enum COMPRESSIONTYPE {
        GZ,
        NONE
    }
   /*
   function：索引不存在就创建索引
   client:Elasticsearch client instance
   indexName:索引名称
   creatorClassName: creator 的类名称
   return void
    */
    private static void mayCreateIndex(Client client, String indexName, String creatorClassName) throws Exception {

        Creator creator = buildCreator(creatorClassName);
        creator.setClient(client);
        IndicesAdminClient adminClient = client.admin().indices();
        IndicesExistsResponse response = adminClient.prepareExists(indexName).execute().actionGet();
        if (!response.isExists()) {
            creator.createIndex(indexName);
        }
    }

    /*
       function :根据类名称获取类实例
       creatorClassName:creator 的类名称
       return Creator;
     */
    private static Creator buildCreator(String creatorClassName) throws Exception {
        Class classType = Class.forName(creatorClassName);
        Object obj = classType.newInstance();
        assert obj instanceof Creator;
        return (Creator)obj;
    }

    /*
     function：根据文件的位置和文件的压缩格式获取Producer
     return：Producer
     */
    private static Producer buildProducer() throws IOException {

        String fs = System.getProperty(BULK_IMPORT_FSTYPE_KEY, FSTYPE.LOCAL.name());
        FSTYPE fstype = FSTYPE.valueOf(fs);

        String compression = System.getProperty(BULK_IMPORT_COMPRESSION_KEY, COMPRESSIONTYPE.NONE.name());
        COMPRESSIONTYPE compressiontype = COMPRESSIONTYPE.valueOf(compression);

        CompressionCodec codec;
        switch (compressiontype) {
            case GZ:
                GzipCodec gzipCodec = new GzipCodec();
                gzipCodec.setConf(new Configuration());
                codec = gzipCodec;
                break;
            default:
                codec = null;
        }
        switch (fstype) {
            case HDFS:
                return new HdfsProducer(codec, new Configuration());
            case LOCAL:
                return new LocalProducer(codec);
            default:
                assert false;
                return null;
        }
    }
    /*
       function :需要cluster name 和unicast host创建elasticsearch client
       return elasticsearch client；
     */
    private static Client buildEsClient() {
        String clusterName = System.getProperty(CLUSTER_NAME_KEY);
        String hostsStr = System.getProperty(UNICAST_HOSTS_KEY);

        if (clusterName == null || hostsStr == null) {
            throw new RuntimeException("Conf missing for elasticsearch");
        }

        Settings settings = Settings.settingsBuilder().put(CLUSTER_NAME_KEY, clusterName).build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        String[] hosts = hostsStr.split(",");
        for (String host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, 9300)));
        }
        return client;
    }
    /*

     */
    private static void load(LoadArg arg) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(arg.concurrent, threadFactory);
        InputStream stream;
        List<Future<?>> futureList = Lists.newLinkedList();
        while ((stream = arg.producer.next()) != null) {
            futureList.add(executorService.submit(new LoadTask(new TaskArg(arg, stream))));
        }

        for (Future<?> future : futureList) {
            try {
                future.get();
            } catch (InterruptedException e) {
                log.error("thread got interrupted", e);
            } catch (ExecutionException e) {
                log.error("import worker exception", e);
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("load finish");
    }
    /*
    加载数据的参数实体类
    client：elasticsearch 的client
    producer：读取数据的producer
    concurrent：并行数量
    batchSize：batch size
    creatorClassName：creator 的类名称
     */
    private static class LoadArg {
        final Client client;
        final Producer producer;
        final int concurrent;
        final int batchSize;
        final String creatorClassName;

        public LoadArg(Client client, Producer producer, int concurrent, int batchSize, String creatorClassName) {
            this.client = client;
            this.producer = producer;
            this.concurrent = concurrent;
            this.batchSize = batchSize;
            this.creatorClassName = creatorClassName;
        }
    }
   /*
   运行任务的参数实体类
   loadArg：数据加载参数
   inputStream：数据输入流
    */
    private static class TaskArg {
        final LoadArg loadArg;
        final InputStream inputStream;

        public TaskArg(LoadArg loadArg, InputStream inputStream) {
            this.loadArg = loadArg;
            this.inputStream = inputStream;
        }
    }

    /*
    加载任务实例
    taskArg：
    creator：
     */
    private static class LoadTask implements Runnable {
            private final TaskArg taskArg;
            private final Creator creator;

            public LoadTask(TaskArg taskArg) throws Exception {
                this.taskArg = taskArg;
                Class classType = Class.forName(taskArg.loadArg.creatorClassName);
                Object obj = classType.newInstance();
                assert obj instanceof Creator;
                this.creator = (Creator)obj;
                this.creator.setClient(taskArg.loadArg.client);
            }

        public void run() {
            long begin = System.currentTimeMillis();
            String fileName = taskArg.inputStream.toString();
            BufferedReader reader = new BufferedReader(new InputStreamReader(taskArg.inputStream));
            try {
                int no = 0;
                String line;
                BulkRequestBuilder bulkRequest = taskArg.loadArg.client.prepareBulk();
                while ((line = reader.readLine()) != null) {
                    IndexRequest request = null;
                    try {
                        request = creator.create(line, no++);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (request != null) {
                        bulkRequest.add(request);
                    }
                    if (bulkRequest.numberOfActions() >= taskArg.loadArg.batchSize) {
                        writeAndHandleRetry(bulkRequest);
                        bulkRequest = taskArg.loadArg.client.prepareBulk();
                    }
                }
                if (bulkRequest.numberOfActions() > 0) {
                    writeAndHandleRetry(bulkRequest);
                }
                log.info("load " + fileName + " finished, cost time:" + (System.currentTimeMillis() - begin) + "ms.");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    taskArg.inputStream.close();
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        /*
        function:在最大重复次数内执行request，如果在最大重试次数以后还有失败的request，抛出来异常
        request:elasticesearch bulk request object
         */
        private void writeAndHandleRetry(BulkRequestBuilder request) throws IOException {
            BulkResponse responses = request.execute().actionGet();
            log.trace("bulk cost time " + responses.getTookInMillis() + "ms");
            int retryTime = 0;
            while ((request = buildRetryRequest(request.request(), responses)) != null
                    && retryTime++ < maxRetryTime) {
                log.warn("retry failed index requests, retry times : " + retryTime);
                try {
                    Thread.sleep(baseWaitTime * Math.min(maxDelayFactor, (int)Math.pow(2, retryTime)));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                responses = request.execute().actionGet();
                log.trace("bulk cost time " + responses.getTookInMillis() + "ms");
            }
            if (request != null) {
                // has fail index request after max retry time
                throwFailedIndexException(responses);
            }
        }
        /*
        function:有失败的request返回重新执行
        request:
        response:
         */
        private BulkRequestBuilder buildRetryRequest(BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                BulkItemResponse[] responses = response.getItems();
                List<ActionRequest> requests = request.requests();
                BulkRequestBuilder newRequest = taskArg.loadArg.client.prepareBulk();
                assert responses.length == requests.size();
                for (int i = 0; i < responses.length; ++i) {
                    if (responses[i].isFailed()) {
                        newRequest.add((IndexRequest)requests.get(i));
                        log.warn(responses[i].getFailureMessage());
                    }
                }
                return newRequest;
            } else {
                return null;
            }
        }
        /*
        function:将elasticsearch 的request请求失败的信息抛出来
        response:
         */
        private void throwFailedIndexException(BulkResponse response) throws IOException {
            StringBuffer sb = new StringBuffer();
            if (response.hasFailures()) {
                BulkItemResponse[] responses = response.getItems();
                for (int i = 0; i < responses.length; ++i) {
                    if (responses[i].isFailed()) {
                        BulkItemResponse.Failure failure = responses[i].getFailure();
                        sb.append("fail index : ").append(failure.getIndex())
                                .append("\ndetail fail message : ").append(failure.getMessage()).append("\n");
                    }
                }
                throw new IOException(sb.toString());
            }
        }
    }
}
