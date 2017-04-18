package io.transwarp.kafkaconsumer;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by 杨发林 on 2016/11/21.
 */
public class Constant {

    /*
     * topic name
     */
    static String KAFKA_TOPIC_NAME;
    /*
     *  每个partition的线程数量
     */
    static String kAFKA_CONSUMER_THREAD_NUM;
    /*
     * 多个topic时，指定topic和对应的线程数量 如：t1:1;t2:5
     */
    private String TOPIC_LIST;
    /*
     * 构造consumer的map对象
     */
    static HashMap<String, Integer> map = new HashMap<String, Integer>();
    /*
     * 读取配置文件
     */
    static Properties p ;

    private Constant(){
        InputStream is = Constant.class.getClassLoader().getResourceAsStream("conf/kafka-set.xml");
        p = new Properties();
        try {
            p.load(is);
            KAFKA_TOPIC_NAME = p.getProperty("kfk.topic");
            kAFKA_CONSUMER_THREAD_NUM = p.getProperty("kfk.consumer.thread.num");
            TOPIC_LIST = p.getProperty("topic.list");
            String topic[] = TOPIC_LIST.split(";");
            for(String t:topic){
                map.put(t.split(":")[0],Integer.parseInt(t.split(":")[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
