package com.datatunnel.sink.hbase

import com.datatunnel.core.BaseConfiguration
import com.datatunnel.core.constant.Constants
import com.datatunnel.core.util.JdbcUtils

import java.sql.ResultSet
import scala.collection.mutable

class HBaseConfiguration extends BaseConfiguration {

  //查询配置的各表rowkey信息
  private val tableAndRowkeys = new mutable.HashMap[String, (Array[String], Array[String])]()

  private val tableAndRowkeysSql =
    s"""
       |select
       | `database`,
       | tablename,
       | hash_key,
       | range_key
       |from rds_pcloud_table_info
       |where ${getString(Constants.SYN_FLAG)} = 1
     """.stripMargin

  JdbcUtils.doQueryHandle(tableAndRowkeysSql, this) ((result: ResultSet) => {
    while (result.next()) {
      val hashKeyArr =
        if(result.getString("hash_key") == null)
          Array.empty[String]
        else
          result.getString("hash_key").toLowerCase.split(",")

      val rangeKeyArr =
        if(result.getString("range_key") == null)
          Array.empty[String]
        else
          result.getString("range_key").toLowerCase.split(",")

      tableAndRowkeys += ((s"${result.getString("database")}_${result.getString("tablename")}",
        (hashKeyArr, rangeKeyArr)
      ))
    }
  })


  def getTableAndRowkeys(): mutable.HashMap[String, (Array[String], Array[String])] = {
    tableAndRowkeys
  }

}
