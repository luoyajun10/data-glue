package com.datatunnel.sink.hbase

import com.datatunnel.core.constant.Constants
import com.datatunnel.core.model.{Insert, Operation, Update}
import com.datatunnel.sink.common.BufferWriter
import com.datatunnel.sink.hbase.util.PrimaryKeyUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, Delete, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * Multi BufferedMutator
  */
class HBaseMultiBufferMutator(config: HBaseConfiguration) extends BufferWriter {
  private val namespace = config.getString(Constants.HBASE_NAMESPACE)
  private var writeBufferSize = config.getString(Constants.HBASE_WRITE_BUFFER_SIZE).toLong
  private val tableMutators = new mutable.HashMap[String, BufferedMutator]()
  private val tableBufferSizes = new mutable.HashMap[String, Long]()
  private val tableKeys = config.getTableAndRowkeys()
  private lazy val connection = ConnectionUtil(config.getString(Constants.HBASE_ZOOKEEPER_QUORUM),
    config.getString(Constants.HBASE_ZOOKEEPER_PORT)).getConnection()

  override def insert(line: Operation): Unit = {
    mutate(line)
  }

  override def update(line: Operation): Unit = {
    mutate(line)
  }

  override def upsert(line: Operation): Unit = {
    mutate(line)
  }

  override def delete(line: Operation): Unit = {
    mutate(line)
  }

  /**
    * 通用的mutate写操作
    * @param line
    */
  def mutate(line: Operation): Unit = {
    // namespace:tablename
    val tableName = s"$namespace${line.getTableName}"

    val bufferedMutator = tableMutators.get(tableName) match {
      case Some(mutator) =>
        mutator
      case None =>
        connection.getBufferedMutator(TableName.valueOf(tableName))
    }

    val rowKey = this.buildRowKey(line, tableKeys)

    if (line.isInstanceOf[Insert] || line.isInstanceOf[Update]) {
      // 添加或更新时put
      val put = new Put(rowKey)

      line.getColumnAndValues.foreach(kv => {
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(kv._1), Bytes.toBytes(kv._2))
      })

      bufferedMutator.mutate(put)
    } else if (line.isInstanceOf[com.datatunnel.core.model.Delete]) {
      // 删除数据时delete
      val delete = new Delete(rowKey)

      bufferedMutator.mutate(delete)
    }

    tableMutators += ((tableName, bufferedMutator))

    this.checkFlush(tableName)
  }

  /**
    * 根据line、表主键信息构建rowkey
    * @param line
    * @param tableKeys
    * @return
    */
  private def buildRowKey(line: Operation, tableKeys: mutable.HashMap[String, (Array[String], Array[String])]): Array[Byte] = {
    val databaseTableName = s"${line.getDatabase}_${line.getTableName}"

    val keys: (Array[String], Array[String]) = tableKeys.get(databaseTableName).get

    val columnValues: mutable.Map[String, String] = line.getColumnAndValues

    var hashKey = StringUtils.EMPTY
    var rangeKey = StringUtils.EMPTY

    keys._1.foreach(key => {
      hashKey += columnValues.get(key).getOrElse("")
    })

    // 如若hashKey值为空 从原始数据主键中获取
    // 注：rds订阅中多个primary键顺序固定, 否则导致落地数据rowkey规则不固定
    if (hashKey.isEmpty){
      line.getPrimaryKeys.foreach(key => {
        hashKey += columnValues.get(key).get
      })
    }

    keys._2.foreach(key => {
      // 如若配置的range_key get不到值便置为空串
      rangeKey += columnValues.get(key).getOrElse("")
    })

    if (rangeKey.isEmpty) {
      PrimaryKeyUtil.buildPrimaryKey(hashKey, PrimaryKeyUtil.TYPE_STRING)
    } else {
      PrimaryKeyUtil.buildPrimaryKey(hashKey, PrimaryKeyUtil.TYPE_STRING, rangeKey, PrimaryKeyUtil.TYPE_STRING)
    }
  }

  /**
    * 检查每张表当前写的buffersize 每达到预设的writeBufferSize即刷写
    * @param tableName
    */
  private def checkFlush(tableName: String): Unit = {
    var currentBufferSize = this.tableBufferSizes.get(tableName).getOrElse(0L)
    currentBufferSize += 1
    tableBufferSizes += ((tableName, currentBufferSize))
    if (currentBufferSize % this.writeBufferSize == 0) {
      this.tableMutators.get(tableName).get.flush
    }
  }

  def getWriteBufferSize(): Long = {
    this.writeBufferSize
  }

  def setWriteBufferSize(size: Long): Unit = {
    this.writeBufferSize = size
  }

  def flush(): Unit = {
    tableMutators.values.foreach(bufferedMutator => {
      bufferedMutator.flush()
    })
  }

  def close(): Unit = {
    tableMutators.values.foreach(bufferedMutator => {
      bufferedMutator.close()
    })
    if (connection != null) {
      connection.close()
    }
  }
}
