package io.transwarp.kafkaconsumer;


import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


/**
 * Created by 杨发林  on 2016/11/21.
 */
/*
 * kafka consumer 的两种典型的消费模式
 * 1，使用topic的白名单功能
 * 2，使用map规定接受消息的topic和对应的每个partition的线程数量
 */
public class KafkaConsumer {
    
    ConsumerConfig consumerConfig = new ConsumerConfig(Constant.p);
    ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    public KafkaConsumer(){}

    public void   whiteListConsumer() throws Exception  {
        Whitelist whitelist = new Whitelist(Constant.KAFKA_TOPIC_NAME);

        List<KafkaStream<byte[], byte[]>> streams = javaConsumerConnector.createMessageStreamsByFilter(whitelist);

        excutorSubmit(streams ,3);
    /*
    if (CollectionUtils.isEmpty(streams)) {
      System.out.println("empty!");
      TimeUnit.SECONDS.sleep(1);
    }
    for (KafkaStream<byte[], byte[]> partition : streams) {
      ConsumerIterator<byte[], byte[]> iterator = partition.iterator();
      while (iterator.hasNext()) {
        MessageAndMetadata<byte[], byte[]> next = iterator.next();

        // TODO 在这里实现自己的consumer逻辑

        System.out.println("partiton:" + next.partition());
        System.out.println("offset:" + next.offset());
        System.out.println("message:" + new String(next.message(), "utf-8"));
      }
    }
*/
    }
    /*
     * 按照规定的每个partition的线程数量启动consumer
     */
    public void mapConsumer() {
        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = javaConsumerConnector.createMessageStreams(Constant.map);
        //List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get("test-topic");
        for (Map.Entry<String, Integer> entry : Constant.map.entrySet()) {
            excutorSubmit(topicMessageStreams.get(entry.getKey()), entry.getValue());
        }

    }

    /*
     * 在每个partition上启动固定数量的线程处理消息
     */
    private void excutorSubmit(List<KafkaStream<byte[], byte[]>> streams, int threadNum) {
        // create list of 4 threads to consume from each of the partitions
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        // TODO 在这里实现自己的consumer逻辑
                        MessageAndMetadata<byte[], byte[]> next = iterator.next();
                        System.out.println("topic : test-topic");
                        System.out.println("partiton:" + next.partition());
                        System.out.println("offset:" + next.offset());
                        try {
                            System.out.println("message:" + new String(next.message(), "utf-8"));

                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }


}
