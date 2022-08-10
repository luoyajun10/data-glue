package com.datatunnel.sink.es.streaming

import com.datatunnel.core.OperationParser
import com.datatunnel.core.constant.Constants
import com.datatunnel.core.filter.TableWhiteListFilter
import com.datatunnel.core.model.Operation
import com.datatunnel.core.util.KafkaConsumerUtils
import com.datatunnel.sink.es.ESConfiguration
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  */
object RdsKafkaToES {

  def main(args: Array[String]): Unit = {

    val userConf = new ESConfiguration

    val sparkConf = new SparkConf().setAppName("RdsKafkaToES")//.setMaster("local[*]")

    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", userConf.getString(Constants.STREAMING_KAFKA_MAXRATEPERPARTITION))

    val ssc = new StreamingContext(sparkConf, Seconds(userConf.getInt(Constants.STREAMING_BATCH_DURATION_SECONDS)))

    //程序内实现监控
    //ssc.addStreamingListener(new KafkaStreamingListener(ssc.sparkContext.applicationId, ssc.sparkContext.appName))

    //广播变量，存放一些配置，表白名单，表结构等信息
    val broadcastConf: Broadcast[ESConfiguration] = ssc.sparkContext.broadcast(userConf)

    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message)

    val inputDStream: InputDStream[(String, Array[Byte])] = KafkaUtils.createDirectStream[String,
      Array[Byte],
      StringDecoder,
      DefaultDecoder,
      (String,Array[Byte])](
      ssc, KafkaConsumerUtils.getKafkaParms(userConf),
      KafkaConsumerUtils.getFromOffsets(userConf),
      messageHandler)

    //记录每批次各个分区的kafka message的offset信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    inputDStream.foreachRDD((rdd: RDD[(String, Array[Byte])]) => {

      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //对msg key进行过滤，这里是表级过滤
      val filterByMsgKeyRDD: RDD[(String, Array[Byte])] = rdd.filter(line => TableWhiteListFilter.filterByKafkaMsgKey(line._1,broadcastConf))

      //对kafka中Protobuffer消息反序列化
      val operationRDD: RDD[Operation] = filterByMsgKeyRDD.map((line: (String, Array[Byte])) => {
        OperationParser.parse(line._2)
      })

      //数据落地
      val handler = new ESHandler

      handler.handle(operationRDD, broadcastConf)

      //rdd每个分区处理完后提交offsets
      handler.commitOffsets(offsetRanges, broadcastConf)

    })


    ssc.start()
    ssc.awaitTermination()
  }

}
