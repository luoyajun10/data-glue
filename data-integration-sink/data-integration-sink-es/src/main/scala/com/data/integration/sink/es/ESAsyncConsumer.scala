package com.data.integration.sink.es

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.client.RestHighLevelClient

import java.util.function.BiConsumer

/**
  *
  */
class ESAsyncConsumer(client: RestHighLevelClient) extends BiConsumer[BulkRequest, ActionListener[BulkResponse]] {

  override def accept(request: BulkRequest, actionListener: ActionListener[BulkResponse]): Unit = {
    client.bulkAsync(request, actionListener)
  }
}
