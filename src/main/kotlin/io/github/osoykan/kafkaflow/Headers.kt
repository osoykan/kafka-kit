package io.github.osoykan.kafkaflow

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader

/**
 * Unified header constants for kafka-flow.
 *
 * These headers are used for retry tracking, error context, and message metadata.
 */
object Headers {
  // ─────────────────────────────────────────────────────────────
  // Retry tracking headers (x- prefix for internal retry tracking)
  // ─────────────────────────────────────────────────────────────

  /** Number of in-memory retry attempts */
  const val RETRY_COUNT = "x-retry-count"

  /** Number of retry topic processing attempts */
  const val RETRY_TOPIC_ATTEMPT = "x-retry-topic-attempt"

  /** Original topic before retry/DLT */
  const val ORIGINAL_TOPIC = "x-original-topic"

  /** Timestamp of first failure */
  const val FIRST_FAILURE_TIME = "x-first-failure-time"

  /** Timestamp of most recent failure */
  const val LAST_FAILURE_TIME = "x-last-failure-time"

  // ─────────────────────────────────────────────────────────────
  // Error context headers (kafka. prefix for error details)
  // ─────────────────────────────────────────────────────────────

  /** Original topic (alternate key for compatibility) */
  const val ERROR_ORIGINAL_TOPIC = "kafka.original.topic"

  /** Original partition */
  const val ORIGINAL_PARTITION = "kafka.original.partition"

  /** Original offset */
  const val ORIGINAL_OFFSET = "kafka.original.offset"

  /** Original timestamp */
  const val ORIGINAL_TIMESTAMP = "kafka.original.timestamp"

  /** Exception class name */
  const val EXCEPTION_CLASS = "kafka.exception.class"

  /** Exception message */
  const val EXCEPTION_MESSAGE = "kafka.exception.message"

  /** Full stack trace (truncated) */
  const val EXCEPTION_STACKTRACE = "kafka.exception.stacktrace"

  /** Immediate cause class and message */
  const val EXCEPTION_CAUSE = "kafka.exception.cause"

  /** Full cause chain */
  const val EXCEPTION_CAUSE_CHAIN = "kafka.exception.cause.chain"

  /** Legacy retry count header */
  const val ERROR_RETRY_COUNT = "kafka.retry.count"

  /** Timestamp when message was sent to DLT */
  const val FAILED_AT = "kafka.failed.at"

  /** Total retry attempts (in-memory + topic) */
  const val TOTAL_RETRY_COUNT = "kafka.total.retry.count"

  /** Processing time in milliseconds */
  const val PROCESSING_TIME_MS = "kafka.processing.time.ms"

  /** In-memory retry count */
  const val IN_MEMORY_RETRY_COUNT = "kafka.inmemory.retry.count"

  /** Retry topic attempt number */
  const val ERROR_RETRY_TOPIC_ATTEMPT = "kafka.retry.topic.attempt"

  /** First failure time (alternate key) */
  const val ERROR_FIRST_FAILURE_TIME = "kafka.first.failure.time"

  /** Consumer name that processed the message */
  const val CONSUMER_NAME = "kafka.consumer.name"
}

/**
 * Extension to get a header value as String from a ConsumerRecord.
 */
fun ConsumerRecord<*, *>.getHeaderString(key: String): String? =
  headers().lastHeader(key)?.value()?.let { String(it) }

/**
 * Extension to get a header value as Int from a ConsumerRecord.
 */
fun ConsumerRecord<*, *>.getHeaderInt(key: String): Int? =
  getHeaderString(key)?.toIntOrNull()

/**
 * Extension to get a header value as Long from a ConsumerRecord.
 */
fun ConsumerRecord<*, *>.getHeaderLong(key: String): Long? =
  getHeaderString(key)?.toLongOrNull()

/**
 * Extension to add a string header to a ProducerRecord.
 */
fun ProducerRecord<*, *>.addHeader(key: String, value: String) {
  headers().add(RecordHeader(key, value.toByteArray()))
}

/**
 * Extension to add an int header to a ProducerRecord.
 */
fun ProducerRecord<*, *>.addHeader(key: String, value: Int) {
  addHeader(key, value.toString())
}

/**
 * Extension to add a long header to a ProducerRecord.
 */
fun ProducerRecord<*, *>.addHeader(key: String, value: Long) {
  addHeader(key, value.toString())
}
