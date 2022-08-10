package com.datatunnel.sink.mysql

import com.datatunnel.core.constant.Constants
import com.datatunnel.core.model.Operation
import com.datatunnel.sink.common.BufferWriter

import java.sql.DriverManager
import scala.collection.mutable.ArrayBuffer

class MySQLBufferWriter(conf: MySQLConfiguration) extends BufferWriter {
  private val url = conf.getString(Constants.MYSQL_JDBC_URL)
  private val user = conf.getString(Constants.MYSQL_JDBC_USER)
  private val passwd = conf.getString(Constants.MYSQL_JDBC_PASSWORD)
  Class.forName("com.mysql.jdbc.Driver")
  private lazy val connection = DriverManager.getConnection(url, user, passwd)
  private lazy val statement = connection.createStatement()
  private var currentBufferSize = 0L
  private var bufferSize = conf.getString(Constants.MYSQL_WRITE_BUFFER_SIZE).toLong
  private var closed = false

  override def insert(line: Operation): Unit = {
    upsert(line)
  }

  override def update(line: Operation): Unit = {
    upsert(line)
  }

  override def upsert(line: Operation): Unit = {

    val table = s"${line.getDatabase}.${line.getTableName}"

    val requires = this.getUpsertRequires(line)

    statement.addBatch(
      s"""
         |INSERT INTO $table (${requires._1}) VALUES (${requires._2})
         |ON DUPLICATE KEY UPDATE ${requires._3}
       """.stripMargin)

    this.checkFlush()
  }

  override def delete(line: Operation): Unit = {

    val table = s"${line.getDatabase}.${line.getTableName}"

    val requires = this.getDeleteRequires(line)

    statement.addBatch(
      s"""
         |DELETE FROM $table WHERE $requires
       """.stripMargin)

    this.checkFlush()
  }

  private def getUpsertRequires(line: Operation): (String, String, String) = {
    val fields = new ArrayBuffer[String]()
    val inserts = new ArrayBuffer[String]()
    val updates = new ArrayBuffer[String]()

    val pks = line.getPrimaryKeys

    line.getColumnAndValues.foreach(kv => {
      val column = kv._1
      val value = kv._2
      fields.append(s"$column")
      inserts.append(s"\'$value\'")

      if (!pks.contains(column)) {
        updates.append(s"$column = \'$value\'")
      }
    })

    (fields.mkString(","), inserts.mkString(","), updates.mkString(","))
  }

  private def getDeleteRequires(line: Operation): String = {
    val deletes = new ArrayBuffer[String]()

    line.getPrimaryKeys.foreach(pk => {
      val value = line.getColumnAndValues.get(pk).get
      deletes.append(s"$pk = \'$value\'")
    })

    deletes.mkString(" and ")
  }

  private def checkFlush(): Unit = {
    this.currentBufferSize += 1
    if (this.currentBufferSize % bufferSize == 0) {
      this.flush()
    }
  }

  override def flush(): Unit = {
    statement.executeBatch()
  }

  override def close(): Unit ={
    if (statement != null && !statement.isClosed) {
      statement.close()
    }
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
    if (connection.isClosed && statement.isClosed) {
      closed = true
    }
  }

  def isClosed(): Boolean = {
    this.closed
  }

  def getBufferSize(): Long = {
    this.bufferSize
  }

  def setBufferSize(size: Long): Unit = {
    this.bufferSize = size
  }
}
