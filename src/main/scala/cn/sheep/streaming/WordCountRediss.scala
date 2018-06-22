package cn.sheep.streaming

import cn.sheep.tools.Jpools
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/14
  */
object WordCountRediss {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

        // 批次时间不是拍脑子定的，是要科学合理的设置？？
        val ssc = new StreamingContext(sparkConf, Seconds(2))


        val stream = ssc.socketTextStream("10.172.50.11", 44444)

        // 计算的当前批次每个单词出现的次数
        val wordCount = stream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


        // 将当前批次的结果写入到redis中
        wordCount.foreachRDD(rdd => {


            rdd.foreachPartition(iter => {
                // 获取jedis连接
                val jedis = Jpools.getJedis

                // 将结果写入到redis
                iter.foreach(tp => {
                    jedis.hincrBy("wordcount", tp._1, tp._2.toLong)
                })

                // 释放连接
                jedis.close()
            })

        })

        ssc.start()
        ssc.awaitTermination()

    }

}
