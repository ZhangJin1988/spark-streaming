package cn.sheep.streaming

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author zhangjin
  * @create 2018-06-14 10:03
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new StreamingContext(conf, Seconds(1))

    //    new StreamingContext(conf,)


    //逻辑 获取数据开始计算
    // 创建一个具有两个工作线程（working thread）并且批次间隔为 1 秒的本地 StreamingContext .
    // master 需要 2 个核, 以防止饥饿情况（starvation scenario）.  一个接受数据 一个处理数据


    //创建一个需要连接到（基础数据源）
    val stream: DStream[String] = sc.socketTextStream("hdp1", 9999)

    //计算wordcount a b c d
    //    val words: DStream[String] = stream.flatMap(_.split(" "))

    //将每个单词 和 1 进行组合 (word,1)
    //    val wordAndOne: DStream[(String, Int)] = words.map(word => (word, 1))

    //分组聚合 统计单词出现的次数
    //    val wordsCount: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //打印结果  触发了action  这个小程序 就非常完美
    //    wordsCount.print()

    //    val unit: Unit = stream.foreachRDD(rdd => {
    //      val result: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //      result.foreachPartition(partition => {
    //        //向数据库写入数据 写入之前要判断一下插入的单词是否已经存在 如果已经存在累加 不存在插入
    //        var conn: Connection = null
    //        var pstm: PreparedStatement = null
    //        try {
    //          val url = "jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8"
    //          conn = DriverManager.getConnection(url, "root", "Zj314159!")
    //          partition.foreach(tp => {
    //            val sql1 = "select * from t_count where word = ?"
    //            pstm = conn.prepareStatement(sql1)
    //            pstm.setString(1, tp._1)
    //            val result1: ResultSet = pstm.executeQuery()
    //            if (result1 != null && result1.next()) {
    //              val sql2 = "update t_count set count = ? where word = ?"
    //              val word = result1.getString("word")
    //              val count = result1.getInt("count") + tp._2
    //              pstm = conn.prepareStatement(sql2)
    //
    //              pstm.setInt(1, count)
    //              pstm.setString(2, word)
    //              val result: Int = pstm.executeUpdate()
    //              println(result)
    //            } else {
    //              val sql3 = "insert into t_count values(?,?)"
    //              pstm = conn.prepareStatement(sql3)
    //              // 赋值
    //              pstm.setString(1, tp._1)
    //              pstm.setInt(2, tp._2)
    //              pstm.execute()
    //            }
    //          }
    //          )
    //        }
    //        catch {
    //          case e: Exception => e.printStackTrace()
    //        }
    //        finally {
    //          if (pstm != null) pstm.close()
    //          if (conn != null) conn.close()
    //        }
    //      })
    //    })
    //    unit


    stream.foreachRDD(rdd => {

      val wordsCount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

      wordsCount.foreachPartition(partition => {
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8&user=root&password=Zj314159!")

        partition.foreach(tp => {
          /**
            * 如果插入的单词 已经存在在数据库中 则更新这个单词的值 当前出现的次数 + 数据库中当前的次数
            * 反之  直接插入即可
            */
          val searchWord: PreparedStatement = conn.prepareStatement("select * from t_word where word = ?")
          searchWord.setString(1, tp._1)
          val resultSet: ResultSet = searchWord.executeQuery()
          if (resultSet.next()) {
            val dbCount: Int = resultSet.getInt("count")
            val newCount: Int = dbCount + tp._2
            //更新
            val updateWord: PreparedStatement = conn.prepareStatement("update t_word set count=? where word = ? ")
            updateWord.setInt(1, newCount)
            updateWord.setString(2, tp._1)
            updateWord.executeUpdate()

          } else {
            //插入的单词不存在
            val pstmt: PreparedStatement = conn.prepareStatement("insert into t_word values(?,?)")
            pstmt.setString(1, tp._1)
            pstmt.setInt(2, tp._2)
            pstmt.executeUpdate()
          }


        })

        conn.close()
      })

    })


    //存储结果

    //释放资源


    sc.start()
    sc.awaitTermination()
  }

}
