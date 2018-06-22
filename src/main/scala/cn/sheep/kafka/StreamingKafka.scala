package cn.sheep.kafka

import cn.sheep.tools.Jpools
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 整合 kafka  wordcount
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/20
  */
object StreamingKafka {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

        // 设置rdd 可以被压缩的
        sparkConf.set("spark.rdd.compress", "true")

        // 设置spark rdd的序列化
        sparkConf.set("spark.serializer", "org.apache.spark.seriliazer.KryoSerializer")

        // 缓慢的关闭jvm,而不是立即停止
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

        // 设置每个分区每分钟可以获取的最大记录条数
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
        /**
          * 批次时间的设置，的依据你的计算逻辑、集群的计算资源
          *
          * batch duration  >= total delay
          */
        val ssc = new StreamingContext(sparkConf, Seconds(2)) // 2 * 10000 * 3


        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "kk-01:9092,kk-02:9092,kk-03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "C-111",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean) // 0.8 self __consumer_offsets
        )

        val topics = Array("wordcount")

        // 从kafka获取数据, 数据是在executor端获取的
        // driver端负责周期性的查询kafka的最新的偏移量范围，然后将偏移量范围交给executor  --- 直连方式
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,// 如果executor和kafka的broker是同一台节点，设置PreferBrokers
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )


        val words: DStream[(String, Int)] = stream.map(crd => (crd.value(), 1))

        words.reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreachPartition(part => {
                val jedis = Jpools.getJedis
                part.foreach(tp => {
                    jedis.incrBy(tp._1, tp._2)
                })
                jedis.close()
            })
        })



        ssc.start()
        ssc.awaitTermination()

    }


}
