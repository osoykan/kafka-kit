package io.github.osoykan.kafkaflow

import kotlin.time.Duration

/**
 * Metrics interface for kafka-flow.
 * Consumer doesn't interact with this directly - it's used internally by supervisors.
 */
interface KafkaFlowMetrics {
  // ─────────────────────────────────────────────────────────────
  // Consumer metrics
  // ─────────────────────────────────────────────────────────────

  /** Record that a message was consumed (before processing) */
  fun recordConsumed(topic: String, consumer: String, partition: Int)

  /** Record successful processing */
  fun recordProcessingSuccess(topic: String, consumer: String, duration: Duration)

  /** Record processing failure (before retry) */
  fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable)

  // ─────────────────────────────────────────────────────────────
  // Retry metrics
  // ─────────────────────────────────────────────────────────────

  /** Record an in-memory retry attempt */
  fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int)

  /** Record message sent to retry topic */
  fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int)

  /** Record message sent to DLT (exhausted all retries) */
  fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int)

  /** Record message expired due to TTL */
  fun recordExpired(topic: String, consumer: String, reason: String)

  // ─────────────────────────────────────────────────────────────
  // Consumer lifecycle metrics
  // ─────────────────────────────────────────────────────────────

  /** Record consumer started */
  fun recordConsumerStarted(consumer: String, topics: List<String>)

  /** Record consumer stopped */
  fun recordConsumerStopped(consumer: String)
}

/**
 * No-op implementation for when metrics are disabled.
 * This is the default implementation used when no metrics registry is provided.
 */
object NoOpMetrics : KafkaFlowMetrics {
  override fun recordConsumed(topic: String, consumer: String, partition: Int) = Unit

  override fun recordProcessingSuccess(topic: String, consumer: String, duration: Duration) = Unit

  override fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable) = Unit

  override fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int) = Unit

  override fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int) = Unit

  override fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int) = Unit

  override fun recordExpired(topic: String, consumer: String, reason: String) = Unit

  override fun recordConsumerStarted(consumer: String, topics: List<String>) = Unit

  override fun recordConsumerStopped(consumer: String) = Unit
}

/**
 * Simple logging-based metrics implementation.
 * Useful for debugging and development.
 */
class LoggingMetrics : KafkaFlowMetrics {
  private val logger = io.github.oshai.kotlinlogging.KotlinLogging
    .logger {}

  override fun recordConsumed(topic: String, consumer: String, partition: Int) {
    logger.debug { "Consumed: topic=$topic, consumer=$consumer, partition=$partition" }
  }

  override fun recordProcessingSuccess(topic: String, consumer: String, duration: Duration) {
    logger.debug { "Success: topic=$topic, consumer=$consumer, duration=$duration" }
  }

  override fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable) {
    logger.debug { "Failure: topic=$topic, consumer=$consumer, exception=${exception::class.simpleName}" }
  }

  override fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int) {
    logger.debug { "In-memory retry: topic=$topic, consumer=$consumer, attempt=$attempt" }
  }

  override fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int) {
    logger.info { "Sent to retry: topic=$topic, retryTopic=$retryTopic, attempt=$attempt" }
  }

  override fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int) {
    logger.warn { "Sent to DLT: topic=$topic, dltTopic=$dltTopic, totalAttempts=$totalAttempts" }
  }

  override fun recordExpired(topic: String, consumer: String, reason: String) {
    logger.warn { "Expired: topic=$topic, consumer=$consumer, reason=$reason" }
  }

  override fun recordConsumerStarted(consumer: String, topics: List<String>) {
    logger.info { "Consumer started: consumer=$consumer, topics=$topics" }
  }

  override fun recordConsumerStopped(consumer: String) {
    logger.info { "Consumer stopped: consumer=$consumer" }
  }
}

/**
 * Composite metrics that delegates to multiple implementations.
 * Useful for combining logging with actual metrics collection.
 */
class CompositeMetrics(
  private val delegates: List<KafkaFlowMetrics>
) : KafkaFlowMetrics {
  constructor(vararg delegates: KafkaFlowMetrics) : this(delegates.toList())

  override fun recordConsumed(topic: String, consumer: String, partition: Int) {
    delegates.forEach { it.recordConsumed(topic, consumer, partition) }
  }

  override fun recordProcessingSuccess(topic: String, consumer: String, duration: Duration) {
    delegates.forEach { it.recordProcessingSuccess(topic, consumer, duration) }
  }

  override fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable) {
    delegates.forEach { it.recordProcessingFailure(topic, consumer, exception) }
  }

  override fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int) {
    delegates.forEach { it.recordInMemoryRetry(topic, consumer, attempt) }
  }

  override fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int) {
    delegates.forEach { it.recordSentToRetryTopic(topic, retryTopic, attempt) }
  }

  override fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int) {
    delegates.forEach { it.recordSentToDlt(topic, dltTopic, totalAttempts) }
  }

  override fun recordExpired(topic: String, consumer: String, reason: String) {
    delegates.forEach { it.recordExpired(topic, consumer, reason) }
  }

  override fun recordConsumerStarted(consumer: String, topics: List<String>) {
    delegates.forEach { it.recordConsumerStarted(consumer, topics) }
  }

  override fun recordConsumerStopped(consumer: String) {
    delegates.forEach { it.recordConsumerStopped(consumer) }
  }
}
