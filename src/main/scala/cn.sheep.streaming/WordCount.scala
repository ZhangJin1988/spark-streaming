package cn.sheep.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream}
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
    val words: DStream[String] = stream.flatMap(_.split(" "))

    //将每个单词 和 1 进行组合 (word,1)
    val wordAndOne: DStream[(String, Int)] = words.map(word => (word, 1))

    //分组聚合 统计单词出现的次数
    val wordsCount: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //打印结果  触发了action  这个小程序 就非常完美
    wordsCount.print()
    //存储结果

    //释放资源


    sc.start()
    sc.awaitTermination()
  }

}
