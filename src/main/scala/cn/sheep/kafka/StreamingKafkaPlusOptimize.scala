package cn.sheep.kafka

import cn.sheep.utils.DBHepler
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: sheep.Old  
  * WeChat: JiaWei-YANG
  * QQ: 64341393 
  * Created 2018/6/20
  */
object StreamingKafkaPlusOptimize {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        // 设置rdd 可以被压缩的
        sparkConf.set("spark.rdd.compress", "true")
        // 设置spark rdd的序列化
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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


        implicit val groupId = "C-111"
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "kk-01:9092,kk-02:9092,kk-03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean) // 0.8 self __consumer_offsets
        )

        val topics = Array("wordcount")


        /**
          * 读取MySQL偏移量信息封装成一个Map[TopicPartition, Long]
          */
        val dbOffsets = DBHepler.getOffset4MySQL(groupId, topics)

        val stream = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent, // 如果executor和kafka的broker是同一台节点，设置PreferBrokers
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, dbOffsets)
        )


        stream.foreachRDD(rdd => {
            // 获取偏移量 driver, 数组的size是多少呢？和kafka分区是一样的
            // 获取偏移量一定是对一手的rdd进行获取
            val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val batchResult = rdd.map(r => (r.value(), 1)).reduceByKey(_ + _).collect()

            /**
              * 将本批次的偏移量插入到MySQL
              */
            DBHepler.saveCurrentOffset2MySQL(ranges)
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
