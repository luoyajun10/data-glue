package com.data.integration.sink.es

import com.data.integration.core.constant.Constants
import com.data.integration.core.model.Operation
import com.data.integration.sink.common.BufferWriter
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.common.xcontent.XContentType

import scala.util.parsing.json.JSONObject

class ESBufferWriter(config: ESConfiguration) extends BufferWriter {
  private val INDEX_OPERATION_TYPE = 0
  private val UPDATE_OPERATION_TYPE = 1
  private val UPSERT_OPERATION_TYPE = 2
  private val DELETE_OPERATION_TYPE = 3
  private var bufferSize = 1000
  private var bufferBytes = 2048000L
  private val concurrent = 0
  private val defaultConflictRetries = 3
  private lazy val client: RestHighLevelClient = this.getRestHighLevelClient()
  private lazy val bulkProcessor = this.getBulkProcessor()
  private val tableAndSinkInfo = config.getTableAndSinkInfo()

  private def getRestHighLevelClient(): RestHighLevelClient = {
    new RestHighLevelClient(RestClient.builder(
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST1), config.getInt(Constants.ES_HTTPHOST_PORT1)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA)),
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST2), config.getInt(Constants.ES_HTTPHOST_PORT2)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA)),
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST3), config.getInt(Constants.ES_HTTPHOST_PORT3)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA)),
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST4), config.getInt(Constants.ES_HTTPHOST_PORT4)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA)),
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST5), config.getInt(Constants.ES_HTTPHOST_PORT5)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA)),
      new HttpHost(config.getString(Constants.ES_HTTPHOST_HOST6), config.getInt(Constants.ES_HTTPHOST_PORT6)
        , config.getString(Constants.ES_HTTPHOST_SCHEMA))
    ))
  }

  private def getBulkProcessor(): BulkProcessor = {
    BulkProcessor.builder(new ESAsyncConsumer(client), new ESProcessorListener())
      .setBulkActions(bufferSize)
      .setBulkSize(new ByteSizeValue(bufferBytes, ByteSizeUnit.BYTES))
      .setConcurrentRequests(concurrent)
      .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
      .build()
  }

  override def insert(line: Operation): Unit = {
    write(line, INDEX_OPERATION_TYPE)
  }

  override def update(line: Operation): Unit = {
    write(line, UPDATE_OPERATION_TYPE)
  }

  override def upsert(line: Operation): Unit = {
    write(line, UPSERT_OPERATION_TYPE)
  }

  override def delete(line: Operation): Unit = {
    write(line, DELETE_OPERATION_TYPE)
  }

  private def write(line: Operation, operationType: Int): Unit = {
    val databaseTableName = s"${line.getDatabase}_${line.getTableName}"

    val esConfig = tableAndSinkInfo.get(databaseTableName).get
    val esIndex = esConfig._1
    val esType = esConfig._2
    val esColumns = esConfig._3

    val dataMap = line.getColumnAndValues
    val primaryKeys = line.getPrimaryKeys

    var id = StringUtils.EMPTY
    primaryKeys.foreach(primaryKey => {
      dataMap.get(primaryKey) match {
        case Some(value) => id += value
        case None => throw new InterruptedException(s"primary key '$primaryKey' get None, data: $dataMap")
      }
    })

    var jsonData = StringUtils.EMPTY

    // 配置了字段列表则只取相应字段值，否则取全部字段
    val subDataMap = new collection.mutable.HashMap[String, String]()
    if (!esColumns.isEmpty) {
      esColumns.foreach(field => {
        val value = dataMap.get(field).getOrElse("")
        subDataMap += ((field, value))
      })
      jsonData = JSONObject(subDataMap.toMap).toString()
    } else {
      jsonData = JSONObject(dataMap.toMap).toString()
    }

    writeOperation(esIndex, esType, id, jsonData, operationType)
  }

  private def writeOperation(esIndex: String, esType: String, _id: String, jsonData: String,
                             operationType: Int): Unit = {
    operationType match {
      case INDEX_OPERATION_TYPE => {
        val request = new IndexRequest(esIndex, esType, _id)
        request.source(jsonData, XContentType.JSON)
        bulkProcessor.add(request)
      }
      case UPDATE_OPERATION_TYPE => {
        val request = new UpdateRequest(esIndex, esType, _id)
        request.doc(jsonData, XContentType.JSON)
        request.retryOnConflict(this.defaultConflictRetries)
        bulkProcessor.add(request)
      }
      case UPSERT_OPERATION_TYPE => {
        val request = new UpdateRequest(esIndex, esType, _id)
        request.doc(jsonData, XContentType.JSON)
        request.upsert(jsonData, XContentType.JSON)
        request.retryOnConflict(this.defaultConflictRetries)
        bulkProcessor.add(request)
      }
      case DELETE_OPERATION_TYPE => {
        val request = new DeleteRequest(esIndex, esType, _id)
        bulkProcessor.add(request)
      }
      case _ =>
    }
  }

  def setBufferSize(size: Int): Unit = {
    this.bufferSize = size
  }

  def setBufferSizes(size: Long): Unit = {
    this.bufferBytes = size
  }

  override def flush(): Unit = {
    bulkProcessor.flush()
  }

  override def close(): Unit = {
    if (bulkProcessor != null) {
      bulkProcessor.close()
    }
    if (client != null) {
      client.close()
    }
  }
}
