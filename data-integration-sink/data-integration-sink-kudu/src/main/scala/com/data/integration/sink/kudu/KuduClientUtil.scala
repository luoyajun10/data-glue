package com.data.integration.sink.kudu

import com.data.integration.sink.common.exception.SinkException
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client._

object KuduClientUtil {
  private var kuduClient: KuduClient = null
  // 支持长连接, token设置5天小于失效时间7天
  private var kuduExpiredClient: KuduClient = null
  private val tokenTimeMillis: Long = 432000000
  private var timeoutMillis: Long = 0

  ShutdownHookManager.get().addShutdownHook(new Runnable {
    println("Shutdown hook is called")
    override def run(): Unit = {
      if (kuduClient != null) kuduClient.close()
      if (kuduExpiredClient != null) kuduExpiredClient.close()
    }
  }, 100)


  def createSyncClient(kuduMaster: String): KuduClient = {
    new KuduClientBuilder(kuduMaster).build()
  }


  def createAsyncClient(kuduMaster: String): AsyncKuduClient = {
    new AsyncKuduClientBuilder(kuduMaster).build()
  }

  /**
    * 并发使用长连接场景
    * Kudu1.5.0之前需要解决Kudu长连接失效的问题, 参考KUDU-2013
    * @return
    */
  def getLongLiveClient(kuduMaster: String): KuduClient = {
    if (kuduClient != null && System.currentTimeMillis() <= timeoutMillis) {
      return kuduClient
    }

    synchronized {
      if (kuduClient == null) {
        timeoutMillis = System.currentTimeMillis() + tokenTimeMillis
        kuduClient = this.createSyncClient(kuduMaster)
        return kuduClient
      } else {
        if (System.currentTimeMillis() <= timeoutMillis) {
          return kuduClient
        }

        if (kuduExpiredClient != null ) {
          try {
            kuduExpiredClient.close()
            kuduExpiredClient = null
          } catch {
            case e: KuduException => throw new SinkException(s"expired client close error! ${e.getMessage}")
          }
        }

        kuduExpiredClient = kuduClient
        timeoutMillis = System.currentTimeMillis() + tokenTimeMillis
        kuduClient = this.createSyncClient(kuduMaster)
        return kuduClient
      }
    }
  }
}
