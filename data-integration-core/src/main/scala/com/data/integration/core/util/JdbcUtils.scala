package com.data.integration.core.util

import com.data.integration.core.BaseConfiguration
import com.data.integration.core.constant.Constants

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object JdbcUtils {

  def doQueryHandle(sql: String, conf: BaseConfiguration)(func: ResultSet => Any): Unit = {

    val url = conf.getString(Constants.META_MYSQL_JDBC_URL)
    val user = conf.getString(Constants.META_MYSQL_JDBC_USER)
    val password = conf.getString(Constants.META_MYSQL_JDBC_PASSWORD)
    Class.forName("com.mysql.jdbc.Driver")

    var connection: Connection = null
    var resultSet: ResultSet = null
    var stat: Statement = null

    try {
      connection = DriverManager.getConnection(url, user, password)
      stat = connection.createStatement()
      resultSet = stat.executeQuery(sql)

      func(resultSet)

    } catch {
      case e: Throwable => throw e
    } finally {
      //TODO 测试要覆盖这里，是否能关闭连接
      if (resultSet != null) resultSet.close()
      if (stat != null) stat.close()
      if (connection != null) connection.close()
    }
  }
}
