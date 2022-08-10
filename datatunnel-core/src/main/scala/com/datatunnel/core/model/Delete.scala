package com.datatunnel.core.model

class Delete(database: String,
             tableName: String,
             primaryKeys: Array[String],
             columnAndValues: scala.collection.mutable.Map[String, String]) extends Operation {

  override def getDatabase: String = database

  override def getTableName: String = tableName

  override def getPrimaryKeys: Array[String] = primaryKeys

  override def getColumnAndValues: scala.collection.mutable.Map[String, String] = columnAndValues
}
