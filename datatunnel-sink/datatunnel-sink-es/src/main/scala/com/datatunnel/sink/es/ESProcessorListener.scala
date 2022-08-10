package com.datatunnel.sink.es

import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.slf4j.LoggerFactory

/**
  *
  */
class ESProcessorListener extends BulkProcessor.Listener {

  private val log = LoggerFactory.getLogger(classOf[ESProcessorListener])

  override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
    val numberOfActions = request.numberOfActions()
    log.info(s"Executing bulk $executionId with $numberOfActions requests")
  }

  override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
    if (response.hasFailures()) {
      log.warn(s"Bulk $executionId executed with failures");
    } else {
      log.info(s"Bulk $executionId completed in ${response.getTook().getMillis()} milliseconds");
    }
  }

  override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
    log.error("Failed to execute bulk", failure)
  }
}
