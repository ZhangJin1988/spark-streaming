package cn.sheep.pratice

import java.util
import java.util.{HashMap, Random, UUID}
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * @author zhangjin
  */
object KafkaProducer {


  private def getRandomWords: String = {
    val assicode: Int = new Random().nextInt(26) + 97
    assicode.toString
  }

  //阻塞的生产者
  def main(args: Array[String]): Unit = {
    val configs = new util.HashMap[String, AnyRef]
    //kafka集群所在的位置
    configs.put("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
    //key 和 value的序列化类
    configs.put("key.serializer", classOf[StringSerializer])
    configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //
    /**
      * acks 应答参数设置
      * all| -1 : follower--》 leader --》 producer
      * 0: 不搭理 leader收到消息后 不做任何响应
      * 1:leader收到消息后 会给producer应答
      */
    configs.put("acks", "1")
    val kafkaProducer = new KafkaProducer[String, String](configs)
    var i = 0
    while (true) {
      Thread.sleep(100l)
      val assicode: Int = new Random().nextInt(26) + 97
      val c: Char = assicode.toChar
//      val str :String = c.toString

      //将消息封装成一个producerRecord
      val str: String = c.toString
      val producerRecord = new ProducerRecord[String, String]("topicA", UUID.randomUUID.toString, getRandomWords)
      val send: Future[RecordMetadata] = kafkaProducer.send(producerRecord)
      System.out.println("this is message " + str)
      i += 1
    }
  }

}
