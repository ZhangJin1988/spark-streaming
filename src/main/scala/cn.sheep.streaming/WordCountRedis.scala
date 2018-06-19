package cn.sheep.streaming

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import cn.sheep.tools.Jtools
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * @author zhangjin
  * @create 2018-06-14 17:45
  */
object WordCountRedis {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new StreamingContext(conf, Seconds(2))
    //创建一个需要连接到（基础数据源）
    val stream: DStream[String] = sc.socketTextStream("hdp1", 9999)
    stream.foreachRDD(rdd => {
      val wordsCount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      wordsCount.foreachPartition(partition => {
        //        val jedis: Jedis = new Jedis("hdp1")

        val jedis: Jedis = Jtools.getJedisPool
        partition.foreach(tp => {
          jedis.hincrBy("wordcount", tp._1, tp._2)
        })
        jedis.close()
      })
    })
    sc.start()
    sc.awaitTermination()
  }

}
