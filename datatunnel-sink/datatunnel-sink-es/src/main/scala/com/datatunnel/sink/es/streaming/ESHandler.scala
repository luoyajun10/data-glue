package com.datatunnel.sink.es.streaming

import com.datatunnel.core.handler.Handler
import com.datatunnel.core.model.{Delete, Insert, Operation, Update}
import com.datatunnel.sink.es.{ESBufferWriter, ESConfiguration}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class ESHandler extends Handler[ESConfiguration] {

  /**
    * handle方法
    *
    * @param rdd rds转换为DStream[Operation]中的rdd
    * @param broadcastConf 应用配置
    */
  override def handle(rdd: RDD[Operation], broadcastConf: Broadcast[ESConfiguration]): Unit = {

    rdd.foreachPartition((iter: Iterator[Operation]) => {

      val config = broadcastConf.value
      val bufferWriter = new ESBufferWriter(config)

      var cnt = 0
      //每个分区内数据处理
      iter.foreach((line: Operation) => {
        line match {
          case insert: Insert => bufferWriter.insert(insert)
          case update: Update => bufferWriter.upsert(update)
          case delete: Delete => bufferWriter.delete(delete)
          case _ =>
        }
        cnt += 1
      })

      bufferWriter.flush()
      bufferWriter.close()
      println(s"partition index: ${TaskContext.getPartitionId()} -- count: $cnt")
    })
  }
}
