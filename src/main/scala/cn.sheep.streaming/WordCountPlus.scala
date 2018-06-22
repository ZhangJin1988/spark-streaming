package cn.sheep.streaming


import cn.sheep.streaming.WordCountStates.updateFunction
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * @author zhangjin
  * @create 2018-06-14 15:28
  */
object WordCountPlus {

//  .setLevel(Level.WARN)

  // function to create new StreamingContext 创建一个新的Context实例
  val functionCreateContext = () => {

    println("-----------")

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(1))

    //    val stream: DStream[String] = ssc.socketTextStream("hdp1", 9999)

    ssc.checkpoint("/Users/zhangjin/myCode/learn/spark-streaming/input")


    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hdp1", 9999)
    val wordCountResult: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunction)


    //数据积压   计算不完 他是永远计算不完的

    wordCountResult.print()
    ssc


  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getSimpleName)

    /**
      * 尝试从Checkpoint目录中 恢复以往的StreamingContext实例
      * 如果恢复不了 则创建一个新的
      */
    val ssc: StreamingContext = StreamingContext.getOrCreate("/Users/zhangjin/myCode/learn/spark-streaming/input", functionCreateContext)
    //    val ssc = new StreamingContext(conf, Seconds(2))


    ssc.start()
    ssc.awaitTermination()
  }

}
