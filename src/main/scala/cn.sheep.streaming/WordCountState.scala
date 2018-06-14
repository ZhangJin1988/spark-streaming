package cn.sheep.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用updateStateByKey来统计某个单词出现的历史状态
  *
  * @author zhangjin
  * @create 2018-06-14 14:55
  */
object WordCountState {

  /**
    * newValues:Seq[Int] : 当前批次某个单词出现的次数 a Seq(1,1,1,1)
    * runningcount :Option[Int]   上次a的值 a=None  或者 Some 历史上出现过 或者 没有出现过
    *
    */
  val updateFunction = (newValues: Seq[Int], runningCount: Option[Int]) => {
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(2))
    //可以将他们放在HDFS上面
    ssc.checkpoint("/Users/zhangjin/myCode/learn/spark-streaming/input")


    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hdp1", 9999)
    val wordCountResult: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunction)


    //数据积压   计算不完 他是永远计算不完的

    wordCountResult.print()


    ssc.start()
    ssc.awaitTermination()


  }

}
