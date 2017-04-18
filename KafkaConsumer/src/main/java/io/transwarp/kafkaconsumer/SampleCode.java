package io.transwarp.kafkaconsumer;

/**
 * Created by 杨发林 on 2016/11/21.
 */
public class SampleCode {
    public static void main(String[] args) throws Exception {
        KafkaConsumer kcConsumer = new KafkaConsumer();
        kcConsumer.whiteListConsumer();
        kcConsumer.mapConsumer();
    }
}
