package com.datatunnel.core.util

import com.datatunnel.core.Configuration
import com.datatunnel.core.constant.Constants
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{OffsetOutOfRangeException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.streaming.kafka.OffsetRange

/**
 * kafka消费者相关工具，主要是offset相关
 * 短连接，使用完即关闭连接
 */
object KafkaConsumerUtils {

  def getKafkaParms(conf: Configuration): Map[String, String] = {
    Map[String, String](
      "metadata.broker.list" -> conf.getString(Constants.KAFKA_BROKER_LIST),
      "bootstrap.servers" -> conf.getString(Constants.KAFKA_BROKER_LIST),
      "group.id" -> conf.getString(Constants.KAFKA_CONSUMER_GROUP_ID),
      "client.id" -> conf.getString(Constants.KAFKA_CONSUMER_CLIENT_ID),
      "auto.offset.reset" -> conf.getString(Constants.KAFKA_CONSUMER_AUTO_OFFSET_RESET)
      //"enable.auto.commit" -> conf.getString(Constants.KAFKA_CONSUMER_ENABLE_AUTO_COMMIT)
    )
  }

  /**
   * 获取fromOffsets
   *
   * @note 兼容kafka 0.9.0
   */
  def getFromOffsets(conf: Configuration): Map[TopicAndPartition, Long] = {

    val fromOffsets = collection.mutable.Map[TopicAndPartition, Long]()

    val zkUtils = getZkUtils(conf.getString(Constants.ZOOKEEPER_CONNECT),
      conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS),
      conf.getInt(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS))

    val zkClient = zkUtils.zkClient

    //topic
    val topic = conf.getString(Constants.KAFKA_CONSUMER_TOPICS)
    //group id
    val groupId = conf.getString(Constants.KAFKA_CONSUMER_GROUP_ID)

    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

    //所有partitions
    val partitions = zkUtils.getPartitionsForTopics(Seq(topic))

    //遍历所有partition，实现topic新增分区容错
    partitions.head._2.foreach(partition => {
      //TopicAndPartition对象
      val topicAndPartition = TopicAndPartition(topic, partition)

      //获取zk保存的offset
      val partitionDir = s"${groupTopicDirs.consumerOffsetDir}/$partition"
      val isExists = zkClient.exists(partitionDir)
      var zkOffset: Long = 0
      if (isExists) {
        val zkData = zkUtils.readDataMaybeNull(partitionDir)._1
        zkData match {
          case Some(data) => zkOffset = data.toLong
          case None => zkOffset = 0
        }
      }

      //获取partition的leader
      val leaderId = zkUtils.getLeaderForPartition(topic, partition).get
      val leader = zkUtils.getBrokerInfo(leaderId).get.endPoints.head._2.host
      val port = zkUtils.getBrokerInfo(leaderId).get.endPoints.head._2.port

      //获取partition当前最小offset
      val minOffset = getKafkaOffset(topic, partition, leader, port, OffsetRequest.EarliestTime)

      //获取partition当前最大offset
      val maxOffset = getKafkaOffset(topic, partition, leader, port, OffsetRequest.LatestTime)

      println(s"zkOffset: $zkOffset  minOffset: $minOffset  maxOffset: $maxOffset")

      //zkOffset小于minOffset说明zk中该partition的offset已过期，或者没有该partition的offset
      if (zkOffset >= minOffset && zkOffset <= maxOffset) {
        fromOffsets += (topicAndPartition -> zkOffset)
      } else if (zkOffset < minOffset) {
        //zkOffset过期时需要根据auto.offset.reset从头消费或最新消费
        val autoOffsetReset = conf.getString(Constants.KAFKA_CONSUMER_AUTO_OFFSET_RESET)

        if ("largest".equals(autoOffsetReset)) {
          fromOffsets += (topicAndPartition -> maxOffset)
        } else if ("smallest".equals(autoOffsetReset)) {
          fromOffsets += (topicAndPartition -> minOffset)
        } else {
          throw new IllegalArgumentException("auto.offset.reset is required, and must be 'largest' or 'smallest'.")
        }

      } else {
        throw new OffsetOutOfRangeException("zkOffset is bigger than maxOffset, is not available.")
      }

    })

    //关闭连接
    zkClient.close()
    zkUtils.close()
    Map[TopicAndPartition, Long]() ++ fromOffsets
  }

  def commitOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
  }

  def commitOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
  }

  def commitOffsets[T <: Configuration](offsetRanges: Array[OffsetRange], conf: T): Unit = {
    val zkUtils = getZkUtils(conf.getString(Constants.ZOOKEEPER_CONNECT),
      conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS),
      conf.getInt(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS))

    val topic = conf.getString(Constants.KAFKA_CONSUMER_TOPICS)

    val groupId = conf.getString(Constants.KAFKA_CONSUMER_GROUP_ID)

    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topic)
    // /consumers/$groupId/offsets/$topic
    val consumerOffsetDir = groupTopicDirs.consumerOffsetDir

    offsetRanges.foreach(offsetRange => {
      //开始offset与结束offset不等说明拉取到了新数据 提交offset到zk
      if (offsetRange.fromOffset != offsetRange.untilOffset) {

        val offsetDir = s"$consumerOffsetDir/${offsetRange.partition}"

        //ZkUtils.updatePersistentPath(zkClient, zkPath, data = offsetRange.untilOffset.toString)
        zkUtils.updatePersistentPath(offsetDir, data = offsetRange.untilOffset.toString)
      }
    })

    zkUtils.zkClient.close()
    zkUtils.close()
  }

  /**
   * 获取ZkUtils
   */
  private def getZkUtils(zkConnection: String, zkSessionTime: Int, zkConnectionTime: Int): ZkUtils = {
    ZkUtils(zkConnection, zkSessionTime, zkConnectionTime, JaasUtils.isZkSecurityEnabled)
  }

  /**
   * 获取kafka partition的offset
   */
  private def getKafkaOffset(topic: String, partition: Int, leader: String, port: Int, time: Long): Long = {
    val consumer = new SimpleConsumer(leader, port, 30000, 65536, "getKafkaOffset")

    val topicAndPartition = TopicAndPartition(topic, partition)
    val offsetRequestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(time, 1))
    val offsetRequest = OffsetRequest(offsetRequestInfo)

    val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
    if (offsetResponse.hasError) {
      throw new RuntimeException(s"failed to get offsets before ${time} -- topic: ${topic}, partition: ${partition}.")
    }

    //offsets类型为Seq[Long] 当前请求参数maxNumOffsets=1因此offsets长度为1
    val offsets = offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets
    val offset = offsets.headOption match {
      case Some(i) => i
      //时间点过早获取不到offset时设置为当前分区最小offset
      case None => getKafkaOffset(topic, partition, leader, port, OffsetRequest.EarliestTime)
    }

    consumer.close()
    offset
  }

}
