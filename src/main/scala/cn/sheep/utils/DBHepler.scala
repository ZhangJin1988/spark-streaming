package cn.sheep.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable

/** 封装数据的操作
  * Author: sheep.Old  
  * WeChat: JiaWei-YANG
  * QQ: 64341393 
  * Created 2018/6/20
  */
object DBHepler {


  DBs.setup()

  // 从数据中获取便宜信息
  def getOffset4MySQL(groupId: String, topics: Array[String]) = {

    // 定义一个map用来装查到的数据
    DB.readOnly { implicit session =>
      SQL("select * from streaming_offset_32 where groupId=? and topics=?")
        .bind(groupId, topics.head)
        .map(rs => (
          new TopicPartition(rs.string("topics"), rs.int("partId")),
          rs.long("offsets")
        )).list().apply()
    }.toMap

  }


  // 将偏移量存储到数据库中
  def saveCurrentOffset2MySQL(offsetRanges: Array[OffsetRange])(implicit groupId: String) = {
    DB.localTx { implicit session =>
      offsetRanges.foreach(r => {
        SQL("replace into streaming_offset_32 values(?,?,?,?)")
          .bind(r.topic, r.partition, r.untilOffset, groupId)
          .update().apply()
      })

      // 批次结果
    }
  }

}
