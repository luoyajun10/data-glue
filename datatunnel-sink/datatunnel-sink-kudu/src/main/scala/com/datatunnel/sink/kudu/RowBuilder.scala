package com.datatunnel.sink.kudu

import com.datatunnel.sink.common.exception.SinkException
import org.apache.kudu.Type
import org.apache.kudu.client.PartialRow

object RowBuilder {

  def buildFunc(row: PartialRow, dataType: Type, column: String, value: String): Unit = {
    //不存在""是类型INT8的数据、否则就有问题了
    if (null == value) {
      row.setNull(column)
    } else {
      dataType match {
        case Type.INT8  => row.addByte(column, value.toByte)
        case Type.INT16 => row.addShort(column, value.toShort)
        case Type.INT32 => row.addInt(column, value.toInt)
        case Type.INT64 => row.addLong(column, value.toLong)
        case Type.UNIXTIME_MICROS => row.addLong(column, value.toLong)
        case Type.STRING => row.addString(column, value)
        case Type.BOOL => row.addBoolean(column, value.toBoolean)
        case Type.FLOAT => row.addFloat(column, value.toFloat)
        case Type.DOUBLE => row.addDouble(column, value.toDouble)
        case _ => throw new SinkException(s"Unsupported type : ${dataType.getName}")
      }
    }
  }
}
