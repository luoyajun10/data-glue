package com.data.integration.sink.hbase.streaming

import com.data.integration.core.handler.Handler
import com.data.integration.core.model.Operation
import com.data.integration.sink.hbase.{ConnectionUtil, HBaseConfiguration, HBaseMultiBufferMutator}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  *
  */
class HBaseHandler extends Handler[HBaseConfiguration] {

  /**
    * handle方法
    *
    * @param rdd rds转换为DStream[Operation]中的rdd
    * @param broadcastConf 应用配置
    */
  override def handle(rdd: RDD[Operation], broadcastConf: Broadcast[HBaseConfiguration]): Unit = {

    rdd.foreachPartition((iter: Iterator[Operation]) => {

      val config = broadcastConf.value
      val multiBuffered = new HBaseMultiBufferMutator(config)

      var cnt = 0
      //每个分区内数据处理
      iter.foreach((line: Operation) => {
        multiBuffered.mutate(line)
        cnt += 1
      })

      multiBuffered.flush()
      multiBuffered.close()
      println(s"partition index: ${TaskContext.getPartitionId()} -- count: $cnt")
    })
  }

}
