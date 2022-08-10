package com.datatunnel.sink.kudu

import com.datatunnel.core.constant.Constants
import com.datatunnel.core.model.Operation
import com.datatunnel.sink.common.BufferWriter
import com.datatunnel.sink.common.exception.SinkException
import org.apache.kudu.client.{KuduException, KuduSession, KuduTable, RowError}
import org.apache.kudu.client.SessionConfiguration.FlushMode

import scala.collection.JavaConversions._
import scala.collection.mutable

class KuduBufferWriter(config: KuduConfiguration) extends BufferWriter {
  private val database = config.getString(Constants.KUDU_DATABASE)
  private val tables = new mutable.HashMap[String, KuduTable]()
  private var bufferSize = 1000
  private var timeoutMillis = 30000L
  private val percentile = 0.8
  private val flushBufferSize = math.ceil(bufferSize * percentile).toInt
  private var currentBufferSize = 0
  private lazy val kuduClient = KuduClientUtil.createSyncClient(config.getString(Constants.KUDU_MASTER))
  private lazy val session = this.newManualFlushKuduSession(bufferSize, timeoutMillis)

  def insert(line: Operation): Unit = {
    val tableName = s"$database${line.getTableName}"
    val table = this.getTable(tableName)
    val insert = table.newInsert()
    val row = insert.getRow

    val dataMap = line.getColumnAndValues

    table.getSchema.getColumns.foreach(column => {
      val colName = column.getName.toLowerCase
      dataMap.get(colName) match {
        case Some(value) => RowBuilder.buildFunc(row, column.getType, colName, value)
        case None =>
      }
    })

    try {
      session.apply(insert)
    } catch {
      case e: KuduException => throw new SinkException(s"insert failed! ${e.getMessage}")
    }
    this.checkFlush()
  }

  def update(line: Operation): Unit = {
    val tableName = s"$database${line.getTableName}"
    val table = this.getTable(tableName)
    val update = table.newUpdate()
    val row = update.getRow

    val dataMap = line.getColumnAndValues

    table.getSchema.getColumns.foreach(column => {
      val colName = column.getName.toLowerCase
      dataMap.get(colName) match {
        case Some(value) => RowBuilder.buildFunc(row, column.getType, colName, value)
        case None =>
      }
    })

    try {
      session.apply(update)
    } catch {
      case e: KuduException => throw new SinkException(s"update failed! ${e.getMessage}")
    }

    this.checkFlush()
  }

  def upsert(line: Operation): Unit = {
    val tableName = s"$database${line.getTableName}"
    val table = this.getTable(tableName)
    val upsert = table.newUpsert()
    val row = upsert.getRow

    val dataMap = line.getColumnAndValues

    table.getSchema.getColumns.foreach(column => {
      val colName = column.getName.toLowerCase
      dataMap.get(colName) match {
        case Some(value) => RowBuilder.buildFunc(row, column.getType, colName, value)
        case None =>
      }
    })

    try {
      session.apply(upsert)
    } catch {
      case e: KuduException => throw new SinkException(s"upsert failed! ${e.getMessage}")
    }
    this.checkFlush()
  }

  def delete(line: Operation): Unit = {
    val tableName = s"$database${line.getTableName}"
    val table = this.getTable(tableName)
    val delete = table.newDelete()
    val row = delete.getRow

    val dataMap = line.getColumnAndValues

    table.getSchema.getPrimaryKeyColumns.map(field => {
      dataMap.get(field.getName) match {
        case Some(value) => RowBuilder.buildFunc(row, field.getType, field.getName, value)
        case None =>
      }
    })

    try {
      session.apply(delete)
    } catch {
      case e: KuduException => throw new SinkException(s"delete failed! ${e.getMessage}")
    }
    this.checkFlush()
  }

  private def getTable(tableName: String): KuduTable = {
    if (tables.contains(tableName)) {
      tables.get(tableName).get
    } else {
      var kuduTable: KuduTable = null
      try {
        kuduTable = kuduClient.openTable(tableName)
      } catch {
        case e: KuduException => throw new SinkException(s"open table error！${e.getMessage}")
      }
      tables += ((tableName, kuduTable))
      kuduTable
    }
  }

  private def newManualFlushKuduSession(bufferSize: Int, timeoutMs: Long): KuduSession = {
    val kuduSession = kuduClient.newSession()
    kuduSession.setFlushMode(FlushMode.MANUAL_FLUSH)
    kuduSession.setMutationBufferSpace(bufferSize)
    kuduSession.setTimeoutMillis(timeoutMs)
    kuduSession
  }

  private def checkFlush(): Unit = {
    this.currentBufferSize += 1
    if (this.currentBufferSize % flushBufferSize == 0) {
      this.flush()
    }
  }

  def flush(): Unit = {
    try{
      this.session.flush()
    }catch {
      case e: KuduException => throw new SinkException(s"session flush error! ${e.getMessage}")
    }
  }

  def close(): Unit = {
    try {
      if (session != null && !session.isClosed) {
        session.close()
      }
      if (kuduClient != null) {
        kuduClient.close()
      }
    } catch {
      case e: KuduException => throw new SinkException(s"Kudu BufferWriter close error! ${e.getMessage}")
    }
  }

  def isClosed(): Boolean = {
    if (this.session != null) {
      this.session.isClosed
    } else {
      true
    }
  }

  def checkFailures(): Unit = {
    val errors = session.getPendingErrors

    if (errors.isOverflowed) {
      throw new SinkException("errors overflow and had to discard row errors!")
    }

    if (errors.getRowErrors.length > 0) {
      val errorDetail = new StringBuilder
      errors.getRowErrors.take(10).foreach((rowError: RowError) => {
        errorDetail.append(rowError.toString).append(";")
      })
      throw new SinkException(s"session flush has RowErrors, details: $errorDetail")
    }
  }

  def setBufferSize(size: Int): Unit = {
    this.bufferSize = size
  }

  def setTimeoutMillis(timeout: Long): Unit = {
    this.timeoutMillis = timeout
  }
}
