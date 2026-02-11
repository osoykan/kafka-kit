package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Result of a send operation.
 */
sealed class SendResult<K : Any, V : Any> {
  /**
   * Successful send result.
   */
  data class Success<K : Any, V : Any>(
    val record: ProducerRecord<K, V>,
    val metadata: RecordMetadata
  ) : SendResult<K, V>()

  /**
   * Failed send result.
   */
  data class Failure<K : Any, V : Any>(
    val record: ProducerRecord<K, V>,
    val exception: Throwable
  ) : SendResult<K, V>()
}

/**
 * Coroutine-friendly Kafka producer that wraps Spring Kafka's KafkaTemplate.
 *
 * Provides suspend functions for sending records with proper error handling.
 *
 * @param kafkaTemplate The Spring Kafka template
 */
class FlowKafkaProducer<K : Any, V : Any>(
  private val kafkaTemplate: KafkaTemplate<K, V>
) {
  private val closed = AtomicBoolean(false)

  /**
   * Sends a single message to the specified topic.
   *
   * @param topic Topic name
   * @param key Message key
   * @param value Message value
   * @return Record metadata on success
   * @throws Exception on send failure
   */
  suspend fun send(topic: String, key: K, value: V): RecordMetadata {
    checkNotClosed()
    logger.debug { "Sending message to topic: $topic with key: $key" }
    return kafkaTemplate.send(topic, key, value).await().recordMetadata
  }

  /**
   * Sends a message with headers to the specified topic.
   *
   * @param topic Topic name
   * @param key Message key
   * @param value Message value
   * @param headers Message headers
   * @return Record metadata on success
   * @throws Exception on send failure
   */
  suspend fun send(
    topic: String,
    key: K,
    value: V,
    headers: Map<String, ByteArray>
  ): RecordMetadata {
    checkNotClosed()
    val record = ProducerRecord<K, V>(topic, null, key, value).apply {
      headers.forEach { (headerKey, headerValue) ->
        headers().add(RecordHeader(headerKey, headerValue))
      }
    }
    logger.debug { "Sending message with headers to topic: $topic with key: $key" }
    return kafkaTemplate.send(record).await().recordMetadata
  }

  /**
   * Sends a message to a specific partition.
   *
   * @param topic Topic name
   * @param partition Partition number
   * @param key Message key
   * @param value Message value
   * @return Record metadata on success
   * @throws Exception on send failure
   */
  suspend fun send(
    topic: String,
    partition: Int,
    key: K,
    value: V
  ): RecordMetadata {
    checkNotClosed()
    logger.debug { "Sending message to topic: $topic partition: $partition with key: $key" }
    return kafkaTemplate.send(topic, partition, key, value).await().recordMetadata
  }

  /**
   * Sends a ProducerRecord directly.
   *
   * @param record The producer record to send
   * @return Record metadata on success
   * @throws Exception on send failure
   */
  suspend fun send(record: ProducerRecord<K, V>): RecordMetadata {
    checkNotClosed()
    logger.debug { "Sending record to topic: ${record.topic()} with key: ${record.key()}" }
    return kafkaTemplate.send(record).await().recordMetadata
  }

  /**
   * Sends multiple records concurrently using coroutines.
   *
   * All records are sent in parallel and the function returns when all have completed.
   * If any send fails, the exception is propagated.
   *
   * @param records List of producer records to send
   * @return List of record metadata for all sends
   * @throws Exception if any send fails
   */
  suspend fun sendAllParallel(records: List<ProducerRecord<K, V>>): List<RecordMetadata> {
    checkNotClosed()
    return coroutineScope {
      records
        .map { record ->
          async { kafkaTemplate.send(record).await().recordMetadata }
        }.awaitAll()
    }
  }

  /**
   * Sends multiple records concurrently with result tracking.
   *
   * All records are sent in parallel. Unlike [sendAllParallel], this captures
   * both successes and failures per record, allowing partial failure handling.
   *
   * @param records List of producer records to send
   * @return List of send results (success or failure for each record)
   */
  suspend fun sendAllParallelWithResults(records: List<ProducerRecord<K, V>>): List<SendResult<K, V>> {
    checkNotClosed()
    return coroutineScope {
      records.map { record -> async { sendWithResult(record) } }.awaitAll()
    }
  }

  /**
   * Flushes any pending sends.
   */
  fun flush() {
    kafkaTemplate.flush()
  }

  /**
   * Closes the producer, preventing further sends.
   */
  fun close() {
    if (closed.compareAndSet(false, true)) {
      logger.info { "Closing FlowKafkaProducer" }
      kafkaTemplate.flush()
    }
  }

  /**
   * Checks if the producer is closed.
   */
  fun isClosed(): Boolean = closed.get()

  private suspend fun sendWithResult(record: ProducerRecord<K, V>): SendResult<K, V> = try {
    val metadata = kafkaTemplate.send(record).await().recordMetadata
    SendResult.Success(record, metadata)
  } catch (e: Exception) {
    logger.error(e) { "Failed to send record to topic: ${record.topic()} with key: ${record.key()}" }
    SendResult.Failure(record, e)
  }

  private fun checkNotClosed() {
    check(!closed.get()) { "FlowKafkaProducer is closed" }
  }
}

/**
 * Extension function to create a ProducerRecord with headers.
 */
fun <K : Any, V : Any> producerRecord(
  topic: String,
  key: K,
  value: V,
  headers: Map<String, ByteArray> = emptyMap(),
  partition: Int? = null,
  timestamp: Long? = null
): ProducerRecord<K, V> = ProducerRecord<K, V>(
  topic,
  partition,
  timestamp,
  key,
  value
).apply {
  headers.forEach { (headerKey, headerValue) ->
    headers().add(RecordHeader(headerKey, headerValue))
  }
}
