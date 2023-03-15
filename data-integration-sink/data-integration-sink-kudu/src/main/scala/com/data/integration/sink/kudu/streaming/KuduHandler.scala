package com.data.integration.sink.kudu.streaming

import com.data.integration.core.constant.Constants
import com.data.integration.core.handler.Handler
import com.data.integration.core.model.{Delete, Insert, Operation, Update}
import com.data.integration.sink.kudu.{KuduBufferWriter, KuduConfiguration}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class KuduHandler extends Handler[KuduConfiguration] {

  /**
    * handle方法
    *
    * @param rdd rds转换为DStream[Operation]中的rdd
    * @param broadcastConf 应用配置
    */
  override def handle(rdd: RDD[Operation], broadcastConf: Broadcast[KuduConfiguration]): Unit = {

    rdd.foreachPartition((iter: Iterator[Operation]) => {

      val config = broadcastConf.value

      val session = new KuduBufferWriter(config)
      session.setBufferSize(config.getInt(Constants.KUDU_SESSION_BUFFER_SPACE))
      session.setTimeoutMillis(config.getInt(Constants.KUDU_SESSION_TIMEOUT_MS))

      var cnt = 0
      iter.foreach((line: Operation) => {
        line match {
          case insert: Insert => session.insert(insert)
          case update: Update => session.upsert(update)
          case delete: Delete => session.delete(delete)
          case _ =>
        }
        cnt += 1
      })

      session.flush()
      session.close()
      println(s"partition -> ${TaskContext.getPartitionId()}, session closed -> ${session.isClosed}, count -> $cnt")
    })
  }

}
