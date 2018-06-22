package cn.sheep.kafka

import java.sql.DriverManager

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/** 自己管理偏移量
  * Author: sheep.Old  
  * WeChat: JiaWei-YANG
  * QQ: 64341393 
  * Created 2018/6/20
  */
object StreamingKafkaPlus {

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


        val groupId = "C-111"
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
          * 启动的时候，的检查自己之前存储的偏移量是什么？----》MySQL
          */
        // 存储偏移量
        val conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
        val preparedStatement = conn1.prepareStatement("select * from streaming_offset_32 where groupId=? and topics=?")
        preparedStatement.setString(1, groupId)
        preparedStatement.setString(2, topics.head)


        // 用来存储从数据里面获取到的偏移量信息
        val offsets = new mutable.HashMap[TopicPartition, Long]()

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
            val topic = resultSet.getString("topics")
            val partId = resultSet.getInt("partId")
            val dbOffsets = resultSet.getLong("offsets")
            offsets += new TopicPartition(topic, partId) -> dbOffsets
        }
        conn1.close()


        val stream = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent, // 如果executor和kafka的broker是同一台节点，设置PreferBrokers
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
        )


        stream.foreachRDD(rdd => {
            // 获取偏移量 driver, 数组的size是多少呢？和kafka分区是一样的
            // 获取偏移量一定是对一手的rdd进行获取
            val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            //
            val batchResult = rdd.map(r => (r.value(), 1)).reduceByKey(_ + _).collect()
             //.foreach(println) -> driver


            val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
            try {
                conn.setAutoCommit(false) // 设置自动提交为false


                val pstmt = conn.prepareStatement("replace into streaming_offset_32 values(?,?,?,?)")
                for (r <- ranges) {
                    pstmt.setString(1, r.topic)
                    pstmt.setInt(2, r.partition)
                    pstmt.setLong(3, r.untilOffset)
                    pstmt.setString(4, groupId)
                    pstmt.executeUpdate()
                }

                conn.commit()

                conn.close()
            } catch {
                case _: Exception => conn.rollback() // 回滚
            }

            // 手动的触发提交偏移量，提交到kafka的__consumer_offsets这个主题下
            // stream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        })


        ssc.start()
        ssc.awaitTermination()
    }

}
