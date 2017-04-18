package io.transwarp.kafkaConsumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class kafkaConsumer {

    public kafkaConsumer() {

    }

    // 线程对象
    private static class Task implements Runnable {
        KafkaStream<byte[], byte[]> stream;

        Task(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[],byte[]> next = it.next();
                System.out.println(Thread.currentThread().getName() + ": partition[" + next.partition() + "],"
                        + "offset[" + next.offset() + "], " + new String(next.message()));
            }
        }
    }

    // 创建线程池，执行kafka消费者
    public void go() {
        Constant constant = new Constant();
        kafkaProperties kafkaProperties = new kafkaProperties();
        ConsumerConfig config = new ConsumerConfig(kafkaProperties.properties());

        ExecutorService executorService = Executors.newFixedThreadPool(Integer.parseInt(constant.THREAD_POOL_SIZE));

        String topic = constant.TOPIC;
//        Task[] tasks = new Task[Integer.parseInt(constant.THREAD_NUM)];
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(constant.THREAD_NUM));
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (KafkaStream stream : streams) {
            executorService.submit(new Task(stream));
        }

        executorService.shutdown();
    }
}
