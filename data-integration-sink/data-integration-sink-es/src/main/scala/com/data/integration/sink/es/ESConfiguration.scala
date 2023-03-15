package com.data.integration.sink.es

import com.data.integration.core.BaseConfiguration
import com.data.integration.core.constant.Constants
import com.data.integration.core.util.JdbcUtils
import org.apache.commons.lang3.StringUtils

import java.sql.ResultSet
import scala.collection.mutable

class ESConfiguration extends BaseConfiguration {

  //查询配置同步的表与落地ES的信息
  private val tableAndSinkInfo = new mutable.HashMap[String, (String, String, Array[String])]()

  private val tableAndSinkInfoSql =
    s"""
       |select
       | `database`,
       | tablename,
       | es_index,
       | es_type,
       | es_columns
       |from rds_pcloud_table_info
       |where ${getString(Constants.SYN_FLAG)} = 1
     """.stripMargin

  JdbcUtils.doQueryHandle(tableAndSinkInfoSql, this) ((result: ResultSet) => {
    while (result.next()) {
      val esIndex =
        if(result.getString("es_index") == null) StringUtils.EMPTY else result.getString("es_index")

      val esType =
        if(result.getString("es_type") == null) StringUtils.EMPTY else result.getString("es_type")

      val esColumns =
        if(result.getString("es_columns") == null)
          Array.empty[String]
        else
          result.getString("es_columns").toLowerCase.split(",")

      tableAndSinkInfo += ((s"${result.getString("database")}_${result.getString("tablename")}",
        (esIndex, esType, esColumns)
      ))
    }
  })


  def getTableAndSinkInfo(): mutable.HashMap[String, (String, String, Array[String])] = {
    tableAndSinkInfo
  }

}
