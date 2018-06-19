package cn.kafka.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * @author zhangjin
 * @create 2018-06-19 15:13
 */
public class ProducerApi {


    @Test
    public void test1() throws InterruptedException {


    }

    public static void main(String[] args) throws InterruptedException {


        //封装producer的参数的
        HashMap<String, Object> configs = new HashMap<>();
        //kafka集群所在的位置
        configs.put("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092");
        //key 和 value的序列化类
        configs.put("key.serializer", StringSerializer.class);
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //
        /**
         * acks 应答参数设置
         *  all| -1 : follower--》 leader --》 producer
         *  0: 不搭理 leader收到消息后 不做任何响应
         *  1:leader收到消息后 会给producer应答
         */
        configs.put("acks", "1");


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(configs);

        int i = 0;
        while (true) {
            Thread.sleep(100l);
            //将消息封装成一个producerRecord
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("hellokafka", UUID.randomUUID().toString(),"this is message" + i);
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            System.out.println("this is message " + i);
            i++;
        }

//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "hdp1:9092");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        for (int i = 0; i < 100; i++)
//            producer.send(new ProducerRecord<>("topic1", Integer.toString(i), Integer.toString(i)));
//
//        producer.close();
    }
}
