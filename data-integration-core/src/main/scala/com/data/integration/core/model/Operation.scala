package com.data.integration.core.model

trait Operation extends Serializable {

  /**
   * 获取库名
   */
  def getDatabase: String

  /**
   * 获取表名
   */
  def getTableName: String

  /**
   * 获取主键
   */
  def getPrimaryKeys: Array[String]

  /**
   * 获取该行记录的所有列和值
   */
  def getColumnAndValues: scala.collection.mutable.Map[String,String]

}
