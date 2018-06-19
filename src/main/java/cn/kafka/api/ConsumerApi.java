package cn.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhangjin
 * @create 2018-06-19 16:05
 */
public class ConsumerApi {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092");
        properties.setProperty("group.id", "c9527");
        /**
         *
         *latest ： 最新的    一开始 就消费最新的数据
         *earliest ： 最早的  一上来就去消费 最早的消息
         *none ：
         */
        properties.setProperty("auto.offset.reset", "earliest");// latest
        //自动提交偏移量
        properties.setProperty("enable.auto.commit", "true");//_consumer_offset 特殊的主题


        /**
         * 消费者 客户端
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        /**
         *制定你要消费的主题 从哪个主题消费数据 可以同时指定多个主题
         */
        consumer.subscribe(Arrays.asList("hellokafka"));
        //拉取下来的数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(3000);
            /**
             * 拉取的消息  全局无序 但是在单个分区里面 是 有序的
             * 只有一个分区的情况下 才可能是全局有序  但是 这样kafka就变成单机的了
             */
            for (ConsumerRecord record : records) {
                System.out.println("record=" + record);
            }
        }


    }
}
