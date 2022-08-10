package com.datatunnel.core

import com.datatunnel.core.exception.ParseException
import com.datatunnel.core.model.{DecodeFunc, Delete, Insert, Operation, TbField, Update}

import scala.collection.JavaConversions.asScalaBuffer

object OperationParser extends Parser[Operation] {

  override def parse(bytes: Array[Byte]): Operation = {

    val rdsModel = DecodeFunc.decode(bytes)

    //库名、表名
    val databaseTableName = rdsModel.getKey.split("\\|")(0).split("_", 2)
    val database = databaseTableName(0).toLowerCase
    val tableName = databaseTableName(1).toLowerCase

    //主键列表
    val primaryKeys = rdsModel.getPrimaryKeys.toLowerCase.split(",")

    val columnValues = scala.collection.mutable.Map[String,String]()
    val oldColumnValues = scala.collection.mutable.Map[String,String]()

    val fields = rdsModel.getFieldList

    //更新操作的数据中有更新前和更新后的数据
    if ("UPDATE".equalsIgnoreCase(rdsModel.getOpt)) {

      fields.zipWithIndex.foreach((line: (TbField, Int)) => {

        //过滤掉null的数据,下游kudu,hbase,mysql不关注null(本来即为null)
        if (line._1.getTbByteString != null) {

          val columnName = line._1.getFieldName.toLowerCase()
          val columnValue = line._1.getTbByteString.toString(line._1.getEncoding).trim

          val isNew = if ((line._2 & 1) == 0) false else true

          if (isNew)
            columnValues += ((columnName,columnValue))
          else
            oldColumnValues += ((columnName,columnValue))
        }
      })

    } else {

      fields.foreach((line: TbField) => {

        if (line.getTbByteString != null) {

          val columnName = line.getFieldName.toLowerCase()
          val columnValue = line.getTbByteString.toString(line.getEncoding).trim

          columnValues += ((columnName,columnValue))
        }
      })

    }

    rdsModel.getOpt.toUpperCase match {
      case "UPDATE" => new Update(database,tableName,primaryKeys,columnValues,oldColumnValues)
      case "DELETE" => new Delete(database,tableName,primaryKeys,columnValues)
      case "INSERT" => new Insert(database,tableName,primaryKeys,columnValues)
      case _ => throw new ParseException(s"Can not recognize operation type : ${rdsModel.getOpt}!")
    }
  }

}
