package io.transwarp.KafkaPartitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class FairPartitioner implements Partitioner {
    public FairPartitioner(VerifiableProperties props) {
    }

    /**
     * Producer在Partitioner里将Key取Hash，
     * 再对Topic的Partition总数取模，得到一个Paritition ID
     * 据此，将msg发送到对应的Paritition
     * @param _key key
     * @param partitions partition
     * @return 取模
     */
    public int partition(Object _key, int partitions) {
        System.out.println("partitions is " + partitions);
        return Integer.parseInt((String)_key) % partitions;
    }
}