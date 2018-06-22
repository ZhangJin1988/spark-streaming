package cn.sheep.streaming

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/14
  */
object WordCounts {

    // 屏蔽日志
    Logger.getLogger("org").setLevel(Level.WARN)


    def main(args: Array[String]): Unit = {

        // 创建一个具有两个工作线程（working thread）并且批次间隔为 2 秒的本地 StreamingContext .
        // master 需要 2 个核, 以防止饥饿情况（starvation scenario）.

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("wordcount")


        // 2 seconds 对数据进行一次切分
        // 启动有别的线程用来接受数据和计算数据
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        // 获取数据 (基础数据源) ReceiverInputDStream DStream子类
        val stream = ssc.socketTextStream("10.172.50.11", 52020)


        // 方式一：以下是使用对DStream进行操作
        //val words = stream.flatMap(_.split(" "))  // 计算 wordcount a b c d
        //val wordAndOne = words.map(word => (word, 1))  // 将单词和1进行组合 （word, 1）
        //val wordsCount = wordAndOne.reduceByKey(_ + _) // 分组聚合，统计单词出现的次数
        //wordsCount.print() // 打印结果，触发action

        // 方式二：将DStream转换成RDD进行操作，和spark core操作方式一样
        stream.foreachRDD(rdd => {


            rdd.foreach(println)

            // 可以选择对DStream操作，也可以选择对RDD进行操作，擅长什么就使用什么
            val wordsCount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _) // 当前批次结果
            wordsCount.foreachPartition(partition => {
                // 获取一根连接 DriverManager.getConnection(....)
                val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=utf-8", "root", "123456")

                /*向数据库中写入数据，写入之前要判断一下插入的单词是否已经存在，如果已存在累加，不存在插入*/
                partition.foreach(tp => {

                    /**
                      * 如果插入的单词已经存在数据库中，则更新这个单词的值（当前出现的次数+数据库当前的次数）
                      * 反之直接插入即可
                      */
                    val searchWord = conn.prepareStatement("select * from wordcount where words = ?")
                    searchWord.setString(1, tp._1)
                    val rs = searchWord.executeQuery()
                    if (rs.next()) {
                        val dbCount = rs.getInt("total")
                        val newCount = dbCount + tp._2

                        // 更新
                        val updateWord = conn.prepareStatement("update wordcount set total=? where words=?")
                        updateWord.setInt(1, newCount)
                        updateWord.setString(2, tp._1)

                        updateWord.executeUpdate()
                    } else {
                        // 插入的单词不存在
                        val pstmt = conn.prepareStatement("insert into wordcount values(?,?)")
                        pstmt.setString(1, tp._1)
                        pstmt.setInt(2, tp._2)

                        pstmt.executeUpdate()
                    }
                })

                // 将连接close
                conn.close()
            })
        })


        // 释放资源
        ssc.start() // 启动spark streaming app
        ssc.awaitTermination() // 等待被终止， main 阻塞
    }

}
