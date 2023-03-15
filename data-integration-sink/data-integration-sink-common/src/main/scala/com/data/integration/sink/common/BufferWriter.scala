package com.data.integration.sink.common

import com.data.integration.core.model.Operation

trait BufferWriter {

  /**
   * 写入操作
   * @param record
   */
  def insert(record: Operation): Unit

  /**
   * 更新操作
   * @param record
   */
  def update(record: Operation): Unit

  /**
   * upsert操作
   * @param record
   */
  def upsert(record: Operation): Unit

  /**
   * 删除操作
   * @param record
   */
  def delete(record: Operation): Unit

  /**
   * 刷写数据
   */
  def flush(): Unit

  /**
   * close操作，比如关闭连接等
   */
  def close(): Unit
}
