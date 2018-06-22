package cn.kafka.practise;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @author zhangjin
 * @create 2018-06-19 18:04
 * wordCount配置数据源 到 kafka
 */
public class PracticeWordCountsByKafka {


    private String getRandomWord() {
        int assicode = new Random().nextInt(26) + 97;
        char word = (char) assicode;
        return word + "";
    }

    //阻塞的生产者
    @Test
    public void testProduce() throws InterruptedException {
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
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topicA", UUID.randomUUID().toString(), getRandomWord());
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            System.out.println("this is message " + i);
            i++;
        }

    }


    /**
     * spark-Streaming 消费 然后 统计单词个数
     */
    @Test
    public void sparkStreamingConsumer() {


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("topicA", "topicB");

//        JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        streamingContext,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );
//
//        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

    }


}
