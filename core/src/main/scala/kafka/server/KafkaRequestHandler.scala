/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.utils.Utils

/**
 * A thread that answers kafka requests.
  * 处理请求的 IO 线程
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: Int,
                         // 注意，这里并不是每个 Processor 线程都有自己的 RequestChannel，仅仅是获取了引用
                          val requestChannel: RequestChannel,
                          apis: KafkaApis) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

  def run() {
    while(true) {
      try {
        var req : RequestChannel.Request = null
        while (req == null) {
          // We use a single meter for aggregate idle percentage for the thread pool.
          // Since meter is calculated as total_recorded_value / time_window and
          // time_window is independent of the number of threads, each recorded idle
          // time should be discounted by # threads.
          val startSelectTime = SystemTime.nanoseconds
          req = requestChannel.receiveRequest(300)
          val idleTime = SystemTime.nanoseconds - startSelectTime
          aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
        }

        if(req eq RequestChannel.AllDone) {
          debug("Kafka request handler %d on broker %d received shut down command".format(
            id, brokerId))
          return
        }
        req.requestDequeueTimeMs = SystemTime.milliseconds
        trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))

        // 根据请求的类型处理请求
        apis.handle(req)
      } catch {
        case e: Throwable => error("Exception when handling request", e)
      }
    }
  }

  def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
}

/**
  * 处理请求的线程池
  * @param brokerId 当前 BrokerId
  * @param requestChannel 封装了请求队列
  * @param apis 各种请求的操作
  * @param numThreads 处理响应的线程的数量，就是配置的 IO 线程的数量
  */
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {

  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "

  /**
    * 维护多个 IO Thread
    */
  val threads = new Array[Thread](numThreads)

  /**
    * 维护多个 IO 线程，KafkaRequestHandler 就是 IO 线程
    */
  val runnables = new Array[KafkaRequestHandler](numThreads)

  /**
    * 根据 IO 线程的数量创建并且运行 IO 线程
    */
  for(i <- 0 until numThreads) {
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()
  }

  def shutdown() {
    info("shutting down")
    for(handler <- runnables)
      handler.shutdown
    for(thread <- threads)
      thread.join
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => scala.collection.Map.empty
    case Some(topic) => Map("topic" -> topic)
  }

  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)
  val bytesInRate = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags)
  val bytesOutRate = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)
  val bytesRejectedRate = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags)
  val failedProduceRequestRate = newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val failedFetchRequestRate = newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val totalProduceRequestRate = newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val totalFetchRequestRate = newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  def close() {
    removeMetric(BrokerTopicStats.MessagesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesOutPerSec, tags)
    removeMetric(BrokerTopicStats.BytesRejectedPerSec, tags)
    removeMetric(BrokerTopicStats.FailedProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.FailedFetchRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalFetchRequestsPerSec, tags)
  }
}

object BrokerTopicStats extends Logging {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"

  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  private val allTopicsStats = new BrokerTopicMetrics(None)

  def getBrokerAllTopicsStats(): BrokerTopicMetrics = allTopicsStats

  def getBrokerTopicStats(topic: String): BrokerTopicMetrics = {
    stats.getAndMaybePut(topic)
  }

  def removeMetrics(topic: String) {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }
}
