package com.datatunnel.core.filter

import com.datatunnel.core.Configuration
import org.apache.spark.broadcast.Broadcast

/**
  * 需要同步的表白名单过滤器
  */
object TableWhiteListFilter {

  /**
    * 通过kafka message key来过滤
    * @param msgKey kafka的message的key
    * @param confBroadcast 应用配置
    * @return
    */
  def filterByKafkaMsgKey[T <: Configuration](msgKey: String, confBroadcast: Broadcast[T]): Boolean = {

    // 从msgKey中获取$database_$tablename
    val databaseTableName = msgKey.split("\\|")(0)
    val opt = msgKey.split("\\|")(1)

    val tableWhiteList = confBroadcast.value.getTableWhiteList
    // 支持特定表忽略DELETE
    val tableIgnoreDeleteList = confBroadcast.value.getTableIgnoreDeleteList

    if (tableWhiteList.contains(databaseTableName)) {
      if (tableIgnoreDeleteList.contains(databaseTableName) && "DELETE".equals(opt))
        false
      else
        true
    } else {
      false
    }
  }

}
