package cn.sheep.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/14
  */
object WordCountPluss {
    // 屏蔽日志
    Logger.getLogger("org").setLevel(Level.WARN)


    /**
      * newValues: Seq[Int]  : 当前批次某个单词出现的次数  a Seq(1,1,1,1)
      * runningCount: Option[Int]: 上次a的值 a=None
      */
    val updateFunction = (newValues: Seq[Int], runningCount: Option[Int]) => {
        val newCount = newValues.sum + runningCount.getOrElse(0)
        Some(newCount)
    }

    // 创建一个新的StreamingContext实例
    val function2CreateContext = () => {
        println("------------------------------")
        val conf = new SparkConf()
        conf.setMaster("local[*]")
        conf.setAppName(this.getClass.getSimpleName)
        val ssc = new StreamingContext(conf, Seconds(2))

        ssc.checkpoint("./ckpt")
        val stream = ssc.socketTextStream("10.172.50.11", 44444)

        val wordCountResult = stream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunction)

        wordCountResult.print()

        ssc
    }

    def main(args: Array[String]): Unit = {

        /**
          * 尝试从checkpoint目录中恢复以往的streamingcontext实例
          * 如果恢复不了，则创建一个新的
          */
        val ssc = StreamingContext.getOrCreate("./ckpt", function2CreateContext)

        ssc.start()
        ssc.awaitTermination()

    }

}
