package com.data.integration.sink.mysql.streaming

import com.data.integration.core.handler.Handler
import com.data.integration.core.model.{Delete, Insert, Operation, Update}
import com.data.integration.sink.mysql.{MySQLBufferWriter, MySQLConfiguration}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class MySQLHandler extends Handler[MySQLConfiguration] {

  /**
    * handle方法
    *
    * @param rdd rds转换为DStream[Operation]中的rdd
    * @param broadcastConf 应用配置
    */
  override def handle(rdd: RDD[Operation], broadcastConf: Broadcast[MySQLConfiguration]): Unit = {

    rdd.foreachPartition((iter: Iterator[Operation]) => {

      val config = broadcastConf.value
      val writer = new MySQLBufferWriter(config)
      writer.setBufferSize(100)

      var cnt = 0
      //每个分区内数据处理
      iter.foreach((line: Operation) => {
        line match {
          case insert: Insert => writer.upsert(insert)
          case update: Update => writer.upsert(update)
          case delete: Delete => writer.delete(delete)
          case _ =>
        }
        cnt += 1
      })

      writer.flush()
      writer.close()
      println(s"partition -> ${TaskContext.getPartitionId()}, writer closed -> ${writer.isClosed()}, count -> $cnt")
    })


  }

}
