package com.datatunnel.core.handler

import com.datatunnel.core.Configuration
import com.datatunnel.core.model.Operation
import com.datatunnel.core.util.KafkaConsumerUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.OffsetRange

trait Handler[T <: Configuration] {

  /**
    * handle方法
    * @param rdd rds转换为DStream[Operation]中的rdd
    * @param broadcastConf 应用配置
    */
  def handle(rdd: RDD[Operation], broadcastConf: Broadcast[T])

  /**
    * 只适用于没有经过shuffle的dstream中的rdd的每个分区的offset提交
    * @param offsetRanges 该批次的所有分区的offset
    * @param broadcastConf 应用配置
    */
  def commitOffsets(offsetRanges: Array[OffsetRange], broadcastConf: Broadcast[T]): Unit = {

    KafkaConsumerUtils.commitOffsets(offsetRanges, broadcastConf.value)

  }

}
