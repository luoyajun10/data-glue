package com.datatunnel.core.listener

import org.apache.spark.streaming.scheduler._

class KafkaStreamingListener(appId : String, appName : String) extends StreamingListener {

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = super.onBatchSubmitted(batchSubmitted)

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    //TODO 监控延时
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = super.onBatchCompleted(batchCompleted)

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = super.onOutputOperationStarted(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = super.onOutputOperationCompleted(outputOperationCompleted)

}
