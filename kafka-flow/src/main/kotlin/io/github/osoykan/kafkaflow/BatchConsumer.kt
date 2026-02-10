package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate

/**
 * Consumer with automatic acknowledgment for batch processing.
 *
 * Receives all records from a single Kafka poll() as a batch.
 * Error handling is explicit via [FailureHandler] -
 * the batch is always considered successful after consume() returns.
 * All offsets are committed after consume() returns.
 *
 * Example:
 * ```kotlin
 * @KafkaTopic(name = "orders.created", retry = "orders.created.retry", dlt = "orders.created.dlt")
 * class OrderBatchConsumer : BatchConsumerAutoAck<String, OrderEvent> {
 *     override suspend fun consume(
 *         records: List<ConsumerRecord<String, OrderEvent>>,
 *         failureHandler: FailureHandler<String, OrderEvent>
 *     ) {
 *         records.forEach { record ->
 *             try {
 *                 orderService.processOrder(record.value())
 *             } catch (e: Exception) {
 *                 failureHandler.sendToDlt(record, e)
 *             }
 *         }
 *     }
 * }
 * ```
 */
interface BatchConsumerAutoAck<K, V> : Consumer<K, V> {
  /**
   * Process a batch of records.
   *
   * @param records The batch of Kafka consumer records from a single poll()
   * @param failureHandler Handler for routing individual failed records to retry/DLT
   */
  suspend fun consume(records: List<ConsumerRecord<K, V>>, failureHandler: FailureHandler<K, V>)
}

/**
 * Consumer with manual acknowledgment for batch processing.
 *
 * Same as [BatchConsumerAutoAck] but you control when offsets are committed
 * by calling [Acknowledgment.acknowledge].
 */
interface BatchConsumerManualAck<K, V> : Consumer<K, V> {
  /**
   * Process a batch of records with manual acknowledgment control.
   *
   * @param records The batch of Kafka consumer records from a single poll()
   * @param failureHandler Handler for routing individual failed records to retry/DLT
   * @param ack Acknowledgment handle - call acknowledge() when done
   */
  suspend fun consume(records: List<ConsumerRecord<K, V>>, failureHandler: FailureHandler<K, V>, ack: Acknowledgment)
}

/**
 * Provides per-record error routing for batch consumers.
 *
 * Use this to route individual records to retry or DLT topics
 * when processing fails within a batch.
 */
interface FailureHandler<K, V> {
  /**
   * Sends a failed record to the retry topic.
   */
  suspend fun sendToRetry(record: ConsumerRecord<K, V>, exception: Throwable)

  /**
   * Sends a failed record to the dead letter topic.
   */
  suspend fun sendToDlt(record: ConsumerRecord<K, V>, exception: Throwable)
}

/**
 * Default implementation of [FailureHandler] that routes records
 * to retry and DLT topics using the resolved consumer configuration.
 */
class DefaultFailureHandler<K : Any, V : Any>(
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val config: ResolvedConsumerConfig
) : FailureHandler<K, V> {
  private val log = KotlinLogging.logger("FailureHandler[${config.consumerName}]")

  override suspend fun sendToRetry(record: ConsumerRecord<K, V>, exception: Throwable) {
    sendToTopic(record, config.retryTopic, exception, filterInternalHeaders = true)
  }

  override suspend fun sendToDlt(record: ConsumerRecord<K, V>, exception: Throwable) {
    sendToTopic(record, config.dltTopic, exception, filterInternalHeaders = false)
  }

  private suspend fun sendToTopic(
    record: ConsumerRecord<K, V>,
    targetTopic: String,
    exception: Throwable,
    filterInternalHeaders: Boolean
  ) {
    try {
      val producerRecord = createErrorRecord(record, targetTopic, exception, filterInternalHeaders)
      kafkaTemplate.send(producerRecord).await()
      log.info { "Sent record to $targetTopic" }
    } catch (e: Exception) {
      log.error(e) { "Failed to send record to $targetTopic" }
      throw e
    }
  }

  private fun createErrorRecord(
    record: ConsumerRecord<K, V>,
    targetTopic: String,
    exception: Throwable,
    filterInternalHeaders: Boolean
  ): ProducerRecord<K, V> {
    val originalTopic = record.getHeaderString(Headers.ORIGINAL_TOPIC) ?: record.topic()
    return ProducerRecord<K, V>(targetTopic, record.key(), record.value()).apply {
      val sourceHeaders = if (filterInternalHeaders) {
        record.headers().filter { !it.key().startsWith("x-") && !it.key().startsWith("kafka.") }
      } else {
        record.headers().toList()
      }
      sourceHeaders.forEach { headers().add(it) }

      addHeader(Headers.ORIGINAL_TOPIC, originalTopic)
      addHeader(Headers.LAST_FAILURE_TIME, System.currentTimeMillis())
      EnrichedErrorContext.from(exception = exception, consumerName = config.consumerName).addHeaders(this)
    }
  }
}
