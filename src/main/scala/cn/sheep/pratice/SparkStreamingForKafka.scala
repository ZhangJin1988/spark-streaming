package cn.sheep.pratice

import cn.sheep.streaming.WordCountPlus.functionCreateContext
import cn.sheep.tools.Jtools
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import redis.clients.jedis.Jedis

/**
  * @author zhangjin
  * @create 2018-06-19 18:13
  */
object SparkStreamingForKafka {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getSimpleName)

    /**
      * 尝试从Checkpoint目录中 恢复以往的StreamingContext实例
      * 如果恢复不了 则创建一个新的
      */
    //    val ssc: StreamingContext = StreamingContext.getOrCreate("/Users/zhangjin/myCode/learn/spark-streaming/input", functionCreateContext)
    val ssc = new StreamingContext(conf, Seconds(2))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hdp1:9092,hdp2:9092,hdp3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "c9527",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("topicA")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val dStream: DStream[(String, Int)] = stream.map(record => (record.value, 1))
    dStream.foreachRDD(rdd => {
      val wordCount: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
      wordCount.foreachPartition(partition => {
        val jedis: Jedis = Jtools.getJedisPool
        partition.foreach(tp => {
          jedis.hincrBy("keys", tp._1, tp._2)
        })
        jedis.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
