package com.data.integration.core

import com.data.integration.core.constant.Constants
import com.data.integration.core.util.JdbcUtils

import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer

/**
  * 个性化配置实现
  * 比如kudu的scheme的获取实现
  */
class BaseConfiguration extends Configuration {

  private val tableWhiteListBuffer = new ArrayBuffer[String]()

  private val tableWhiteListSql : String =
    s"""
      |select
      | `database`,
      | tablename
      |from rds_pcloud_table_info
      |where ${getString(Constants.SYN_FLAG)} = 1
    """.stripMargin

  JdbcUtils.doQueryHandle(tableWhiteListSql, this)((resultSet: ResultSet) => {
    while (resultSet.next()) {
      tableWhiteListBuffer.append(s"${resultSet.getString("database").toLowerCase}_${resultSet.getString("tablename").toLowerCase}")
    }
  })

  // 忽略DELETE的表白名单。这里后面可以配置到rds_pcloud_table_info
  private val tableIgnoreDeleteList = Array("xxx")

  // 需要设计从哪里获取白名单的配置
  // 需测  表名白名单 与 kafka中key的拼接一致，"小写库名_小写表名"
  override def getTableWhiteList: Array[String] = {
    tableWhiteListBuffer.toArray
  }

  override def getTableIgnoreDeleteList: Array[String] = {
    tableIgnoreDeleteList
  }

}
