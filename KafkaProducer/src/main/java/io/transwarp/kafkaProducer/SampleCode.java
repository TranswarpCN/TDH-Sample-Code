package io.transwarp.kafkaProducer;

public class SampleCode {
    // 主函数
    public static void main(String[] args) {
        // ./kafka-create-topic.sh --zookeeper 172.16.2.93:2181 --partition 3 --topic tmp_kafka_1st --broker 172.16.2.93 172.16.2.94 172.16.2.95
        kafkaProducer kafkaProducer = new kafkaProducer();
        kafkaProducer.go();
    }
}
