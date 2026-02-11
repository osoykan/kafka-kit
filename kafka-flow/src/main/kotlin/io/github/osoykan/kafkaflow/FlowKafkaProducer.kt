package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

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
    return records
      .map { record -> kafkaTemplate.send(record).asDeferred() }
      .awaitAll()
      .map { it.recordMetadata }
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

  private fun checkNotClosed() {
    check(!closed.get()) { "FlowKafkaProducer is closed" }
  }
}
