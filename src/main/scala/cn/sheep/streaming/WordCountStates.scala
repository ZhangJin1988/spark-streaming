package cn.sheep.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** 使用updateStateByKey统计某个单词出现的历史状态
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/14
  */
object WordCountStates {

    /**
      * newValues: Seq[Int]  : 当前批次某个单词出现的次数  a Seq(1,1,1,1)
      * runningCount: Option[Int]: 上次a的值 a=None
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
        // 设置检查点目录，用于存储中间数据结果 hdfs
        /**
          * Metadata checkpointing - 将定义 streaming 计算的信息保存到容错存储（如 HDFS）中.这用于从运行 streaming 应用程序的 driver 的节点的故障中恢复（稍后详细讨论）. 元数据包括:
          *     Configuration - 用于创建流应用程序的配置.
          *     DStream operations - 定义 streaming 应用程序的 DStream 操作集.
          *     Incomplete batches - 批量的job 排队但尚未完成.
          *
          * Data checkpointing - 将生成的 RDD 保存到可靠的存储.这在一些将多个批次之间的数据进行组合的 状态 变换中是必需的.在这种转换中, 生成的 RDD 依赖于先前批次的 RDD, 这导致依赖链的长度随时间而增加.为了避免恢复时间的这种无限增加（与依赖关系链成比例）, 有状态转换的中间 RDD 会定期 checkpoint 到可靠的存储（例如 HDFS）以切断依赖关系链.
          */
        ssc.checkpoint("./ckpt")

        val stream = ssc.socketTextStream("10.172.50.11", 44444)
//        stream.checkpoint(2)  batchTime 10

        val wordCountResult = stream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunction)

        wordCountResult.print()

        ssc.start()
        ssc.awaitTermination()

    }

}
