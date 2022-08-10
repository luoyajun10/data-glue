package com.datatunnel.core.constant

object Constants {

  /**
   * mysql元数据配置
   */
  val META_MYSQL_JDBC_URL = "meta.mysql.jdbc.url"
  val META_MYSQL_JDBC_USER = "meta.mysql.jdbc.user"
  val META_MYSQL_JDBC_PASSWORD = "meta.mysql.jdbc.password"

  /**
   * mysql是否同步字段
   */
  val SYN_FLAG = "syn_flag"


  /**
   * zookeeper配置
   */
  val ZOOKEEPER_CONNECT = "zookeeper.connect"
  val ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms"
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms"


  /**
   * kafka配置
   */
  val KAFKA_BROKER_LIST = "kafka.broker.list"
  val KAFKA_CONSUMER_TOPICS = "kafka.consumer.topics"
  val KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group.id"
  val KAFKA_CONSUMER_CLIENT_ID = "kafka.consumer.client.id"
  val KAFKA_CONSUMER_AUTO_OFFSET_RESET = "kafka.consumer.auto.offset.reset"
  val KAFKA_CONSUMER_SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms"
  val KAFKA_CONSUMER_SOCKET_RECEIVE_BUFFER_BYTES = "kafka.consumer.socket.receive.buffer.bytes"
  val KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = "kafka.consumer.enable.auto.commit"

  /**
   * kudu配置
   */
  val KUDU_MASTER = "kudu.master"
  val KUDU_DATABASE = "kudu.database"
  val KUDU_SESSION_BUFFER_SPACE = "kudu.session.buffer.space"
  val KUDU_SESSION_TIMEOUT_MS = "kudu.session.timeout.ms"

  /**
   * hbase配置
   */
  val HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
  val HBASE_ZOOKEEPER_PORT = "hbase.zookeeper.port"
  val HBASE_NAMESPACE = "hbase.namespace"
  val HBASE_WRITE_BUFFER_SIZE = "hbase.write.buffer.size"

  /**
   * Es配置
   */
  val ES_HTTPHOST_HOST1 = "es.httpHost.host1"
  val ES_HTTPHOST_HOST2 = "es.httpHost.host2"
  val ES_HTTPHOST_HOST3 = "es.httpHost.host3"
  val ES_HTTPHOST_HOST4 = "es.httpHost.host4"
  val ES_HTTPHOST_HOST5 = "es.httpHost.host5"
  val ES_HTTPHOST_HOST6 = "es.httpHost.host6"
  val ES_HTTPHOST_PORT1 = "es.httpHost.port1"
  val ES_HTTPHOST_PORT2 = "es.httpHost.port2"
  val ES_HTTPHOST_PORT3 = "es.httpHost.port3"
  val ES_HTTPHOST_PORT4 = "es.httpHost.port4"
  val ES_HTTPHOST_PORT5 = "es.httpHost.port5"
  val ES_HTTPHOST_PORT6 = "es.httpHost.port6"
  val ES_HTTPHOST_SCHEMA = "es.httpHost.schema"


  /**
   * mysql配置
   */
  val MYSQL_JDBC_URL = "mysql.jdbc.url"
  val MYSQL_JDBC_USER = "mysql.jdbc.user"
  val MYSQL_JDBC_PASSWORD = "mysql.jdbc.password"
  val MYSQL_WRITE_BUFFER_SIZE = "mysql.write.buffer.size"

  /**
   * spark配置
   * - spark.streaming.batch.duration.seconds: streaming 批次间隔时间
   * - spark.streaming.kafka.maxRatePerPartition: streaming kafka流控
   */
  val STREAMING_BATCH_DURATION_SECONDS = "spark.streaming.batch.duration.seconds"
  val STREAMING_KAFKA_MAXRATEPERPARTITION = "spark.streaming.kafka.maxRatePerPartition"
}
