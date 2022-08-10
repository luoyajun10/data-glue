package com.datatunnel.sink.hbase

import com.datatunnel.sink.common.exception.SinkException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, RegionLocator, Table}

import java.io.IOException

class ConnectionUtil(zkHosts: String, zkPort: String, zkNode: String) {

  private var connection: Connection = null
  private var conf: Configuration = null

  try {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkHosts)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf.set("zookeeper.znode.parent", zkNode)
    connection = ConnectionFactory.createConnection(conf)
  } catch {
    case e: IOException => throw new SinkException(e.getMessage)
  }

  def getConnection(): Connection = {
    this.connection
  }

  def getConfiguration(): Configuration = {
    this.conf
  }

  def getAdmin(): Admin = {
    this.connection.getAdmin
  }

  def getTable(tableName: String): Table = {
    this.connection.getTable(TableName.valueOf(tableName))
  }

  def getBufferedMutator(tableName: String): BufferedMutator = {
    this.connection.getBufferedMutator(TableName.valueOf(tableName))
  }

  def getBufferedMutator(mutatorParams: BufferedMutatorParams): BufferedMutator = {
    this.connection.getBufferedMutator(mutatorParams)
  }

  def getRegionLocator(tableName: String): RegionLocator = {
    this.connection.getRegionLocator(TableName.valueOf(tableName))
  }

  def close(): Unit = {
    synchronized {
      if (connection != null && !connection.isClosed){
        connection.close()
      }
    }
  }

}

object ConnectionUtil {

  def apply(zkHosts: String, zkPort: String, zkNode: String = "/hbase"): ConnectionUtil = {
    new ConnectionUtil(zkHosts, zkPort, zkNode)
  }
}
