package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.FixedBackOff
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger {}

/**
 * Enriched error context with detailed exception information.
 * Used for creating comprehensive error headers in retry/DLT records.
 */
data class EnrichedErrorContext(
  val exception: Throwable,
  val stackTrace: String,
  val causeChain: List<String>,
  val processingTimeMs: Long,
  val attemptNumber: Int,
  val totalRetries: Int,
  val inMemoryRetryCount: Int,
  val retryTopicAttempt: Int,
  val consumerName: String? = null
) {
  companion object {
    /**
     * Creates an EnrichedErrorContext from an exception.
     */
    fun from(
      exception: Throwable,
      processingTimeMs: Long = 0,
      attemptNumber: Int = 0,
      totalRetries: Int = 0,
      inMemoryRetryCount: Int = 0,
      retryTopicAttempt: Int = 0,
      consumerName: String? = null,
      maxStackTraceDepth: Int = 50
    ): EnrichedErrorContext {
      val stackTrace = exception.stackTrace
        .take(maxStackTraceDepth)
        .joinToString("\n") { "\tat $it" }

      val causeChain = buildList {
        var cause: Throwable? = exception.cause
        var depth = 0
        while (cause != null && depth < 10) {
          add("${cause::class.qualifiedName ?: cause::class.java.name}: ${cause.message ?: "no message"}")
          cause = cause.cause
          depth++
        }
      }

      return EnrichedErrorContext(
        exception = exception,
        stackTrace = stackTrace,
        causeChain = causeChain,
        processingTimeMs = processingTimeMs,
        attemptNumber = attemptNumber,
        totalRetries = totalRetries,
        inMemoryRetryCount = inMemoryRetryCount,
        retryTopicAttempt = retryTopicAttempt,
        consumerName = consumerName
      )
    }
  }

  /**
   * Adds all error context headers to a ProducerRecord.
   */
  fun addHeaders(record: org.apache.kafka.clients.producer.ProducerRecord<*, *>) {
    record.headers().apply {
      add(RecordHeader(Headers.EXCEPTION_CLASS, exception::class.java.name.toByteArray()))
      add(RecordHeader(Headers.EXCEPTION_MESSAGE, (exception.message ?: "").toByteArray()))
      add(RecordHeader(Headers.EXCEPTION_STACKTRACE, stackTrace.toByteArray()))

      if (causeChain.isNotEmpty()) {
        add(RecordHeader(Headers.EXCEPTION_CAUSE, causeChain.first().toByteArray()))
        add(RecordHeader(Headers.EXCEPTION_CAUSE_CHAIN, causeChain.joinToString(" -> ").toByteArray()))
      }

      add(RecordHeader(Headers.PROCESSING_TIME_MS, processingTimeMs.toString().toByteArray()))
      add(RecordHeader(Headers.TOTAL_RETRY_COUNT, totalRetries.toString().toByteArray()))
      add(RecordHeader(Headers.IN_MEMORY_RETRY_COUNT, inMemoryRetryCount.toString().toByteArray()))
      add(RecordHeader(Headers.ERROR_RETRY_TOPIC_ATTEMPT, retryTopicAttempt.toString().toByteArray()))

      consumerName?.let {
        add(RecordHeader(Headers.CONSUMER_NAME, it.toByteArray()))
      }
    }
  }
}

/**
 * Configuration for Spring Kafka error handling.
 *
 * For the lean consumer pattern, use [RetryPolicy] with [RetryableProcessor] instead.
 * This config is primarily for creating Spring Kafka [CommonErrorHandler] instances.
 *
 * @property maxRetries Maximum number of retries before sending to DLT
 * @property retryBackoff Duration between retries
 * @property dltTopicSuffix Suffix for dead letter topic (e.g., ".dlt")
 * @property retryTopicSuffix Suffix for retry topic (e.g., ".retry")
 */
data class ErrorHandlerConfig(
  val maxRetries: Int = 3,
  val retryBackoff: Duration = 1.seconds,
  val dltTopicSuffix: String = ".dlt",
  val retryTopicSuffix: String = ".retry"
) {
  companion object {
    /**
     * Creates an ErrorHandlerConfig from a RetryPolicy for consistency.
     */
    fun fromRetryPolicy(policy: RetryPolicy): ErrorHandlerConfig {
      val backoffDelay = when (val backoff = policy.inMemoryBackoff) {
        is BackoffStrategy.Fixed -> backoff.delay
        is BackoffStrategy.Exponential -> backoff.initialDelay
        is BackoffStrategy.None -> 1.seconds
      }
      return ErrorHandlerConfig(
        maxRetries = policy.maxInMemoryRetries,
        retryBackoff = backoffDelay,
        dltTopicSuffix = policy.dltSuffix,
        retryTopicSuffix = policy.retryTopicSuffix
      )
    }
  }
}

/**
 * Creates a Spring Kafka CommonErrorHandler with configurable retry and DLT support.
 *
 * @param config Error handler configuration
 * @param kafkaTemplate Template for sending to retry/DLT topics
 * @param recoverer Optional custom recoverer for DLT messages
 * @return Configured error handler
 */
fun <K : Any, V : Any> createErrorHandler(
  config: ErrorHandlerConfig = ErrorHandlerConfig(),
  kafkaTemplate: KafkaTemplate<K, V>? = null,
  recoverer: ((ConsumerRecord<*, *>, Exception) -> Unit)? = null
): CommonErrorHandler {
  val backOff: BackOff = FixedBackOff(config.retryBackoff.inWholeMilliseconds, config.maxRetries.toLong())

  val actualRecoverer: (ConsumerRecord<*, *>, Exception) -> Unit = recoverer ?: { record, exception ->
    if (kafkaTemplate != null) {
      val dltTopic = record.topic() + config.dltTopicSuffix
      @Suppress("UNCHECKED_CAST")
      sendToDlt(kafkaTemplate, record as ConsumerRecord<Any, Any>, dltTopic, exception)
    } else {
      logger.error(exception) {
        "Message recovery failed for topic: ${record.topic()}, " +
          "partition: ${record.partition()}, offset: ${record.offset()}"
      }
    }
  }

  return DefaultErrorHandler(actualRecoverer, backOff)
}

/**
 * Sends a failed message to the dead letter topic.
 */
@Suppress("UNCHECKED_CAST")
private fun <K : Any, V : Any> sendToDlt(
  kafkaTemplate: KafkaTemplate<K, V>,
  record: ConsumerRecord<Any, Any>,
  dltTopic: String,
  exception: Exception
) {
  try {
    val dltRecord = ProducerRecord<K, V>(
      dltTopic,
      null,
      record.key() as? K,
      record.value() as? V
    ).apply {
      // Copy original headers
      record.headers().forEach { header ->
        headers().add(header)
      }
      // Add error information headers
      headers().add(RecordHeader(Headers.ERROR_ORIGINAL_TOPIC, record.topic().toByteArray()))
      headers().add(RecordHeader(Headers.ORIGINAL_PARTITION, record.partition().toString().toByteArray()))
      headers().add(RecordHeader(Headers.ORIGINAL_OFFSET, record.offset().toString().toByteArray()))
      headers().add(RecordHeader(Headers.ORIGINAL_TIMESTAMP, record.timestamp().toString().toByteArray()))
      headers().add(RecordHeader(Headers.EXCEPTION_CLASS, exception.javaClass.name.toByteArray()))
      headers().add(RecordHeader(Headers.EXCEPTION_MESSAGE, (exception.message ?: "").toByteArray()))
      headers().add(RecordHeader(Headers.FAILED_AT, System.currentTimeMillis().toString().toByteArray()))
    }

    kafkaTemplate.send(dltRecord)
    logger.info { "Sent failed message to DLT: $dltTopic" }
  } catch (e: Exception) {
    logger.error(e) { "Failed to send message to DLT: $dltTopic" }
  }
}

/**
 * Dead Letter Topic handler that processes messages from DLT.
 */
class DltHandler<K : Any, V : Any>(
  private val handler: suspend (ConsumerRecord<K, V>, DltMetadata) -> Unit
) {
  /**
   * Processes a DLT record, extracting metadata from headers.
   */
  suspend fun handle(record: ConsumerRecord<K, V>) {
    val metadata = extractDltMetadata(record)
    handler(record, metadata)
  }

  private fun extractDltMetadata(record: ConsumerRecord<K, V>): DltMetadata {
    fun getHeader(name: String): String? =
      record
        .headers()
        .lastHeader(name)
        ?.value()
        ?.let { String(it) }

    return DltMetadata(
      originalTopic = getHeader(Headers.ERROR_ORIGINAL_TOPIC),
      originalPartition = getHeader(Headers.ORIGINAL_PARTITION)?.toIntOrNull(),
      originalOffset = getHeader(Headers.ORIGINAL_OFFSET)?.toLongOrNull(),
      originalTimestamp = getHeader(Headers.ORIGINAL_TIMESTAMP)?.toLongOrNull(),
      exceptionClass = getHeader(Headers.EXCEPTION_CLASS),
      exceptionMessage = getHeader(Headers.EXCEPTION_MESSAGE),
      failedAt = getHeader(Headers.FAILED_AT)?.toLongOrNull(),
      // Enhanced error context
      exceptionStacktrace = getHeader(Headers.EXCEPTION_STACKTRACE),
      exceptionCause = getHeader(Headers.EXCEPTION_CAUSE),
      exceptionCauseChain = getHeader(Headers.EXCEPTION_CAUSE_CHAIN),
      totalRetryCount = getHeader(Headers.TOTAL_RETRY_COUNT)?.toIntOrNull(),
      processingTimeMs = getHeader(Headers.PROCESSING_TIME_MS)?.toLongOrNull(),
      inMemoryRetryCount = getHeader(Headers.IN_MEMORY_RETRY_COUNT)?.toIntOrNull(),
      retryTopicAttempt = getHeader(Headers.ERROR_RETRY_TOPIC_ATTEMPT)?.toIntOrNull(),
      firstFailureTime = getHeader(Headers.ERROR_FIRST_FAILURE_TIME)?.toLongOrNull(),
      consumerName = getHeader(Headers.CONSUMER_NAME)
    )
  }
}

/**
 * Metadata about a dead letter message.
 */
data class DltMetadata(
  val originalTopic: String?,
  val originalPartition: Int?,
  val originalOffset: Long?,
  val originalTimestamp: Long?,
  val exceptionClass: String?,
  val exceptionMessage: String?,
  val failedAt: Long?,
  // Enhanced error context
  val exceptionStacktrace: String? = null,
  val exceptionCause: String? = null,
  val exceptionCauseChain: String? = null,
  val totalRetryCount: Int? = null,
  val processingTimeMs: Long? = null,
  val inMemoryRetryCount: Int? = null,
  val retryTopicAttempt: Int? = null,
  val firstFailureTime: Long? = null,
  val consumerName: String? = null
)

/**
 * Retry handler that can republish messages to retry topics.
 */
class RetryHandler<K : Any, V : Any>(
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val config: ErrorHandlerConfig = ErrorHandlerConfig()
) {
  /**
   * Sends a record to its retry topic.
   *
   * @param record The record to retry
   * @param retryCount Current retry count
   * @return true if sent successfully
   */
  suspend fun sendToRetry(record: ConsumerRecord<K, V>, retryCount: Int): Boolean = try {
    val retryTopic = record.topic() + config.retryTopicSuffix
    val retryRecord = ProducerRecord<K, V>(
      retryTopic,
      record.partition(),
      record.key(),
      record.value()
    ).apply {
      record.headers().forEach { header ->
        headers().add(header)
      }
      headers().add(RecordHeader(Headers.ERROR_RETRY_COUNT, retryCount.toString().toByteArray()))
      headers().add(RecordHeader(Headers.ERROR_ORIGINAL_TOPIC, record.topic().toByteArray()))
    }

    kafkaTemplate.send(retryRecord).get()
    logger.info { "Sent message to retry topic: $retryTopic, retry count: $retryCount" }
    true
  } catch (e: Exception) {
    logger.error(e) { "Failed to send message to retry topic" }
    false
  }

  /**
   * Gets the retry count from a record's headers.
   */
  fun getRetryCount(record: ConsumerRecord<K, V>): Int = record
    .headers()
    .lastHeader(Headers.ERROR_RETRY_COUNT)
    ?.value()
    ?.let { String(it).toIntOrNull() }
    ?: 0

  /**
   * Checks if a record has exceeded the maximum retry count.
   */
  fun hasExceededMaxRetries(record: ConsumerRecord<K, V>): Boolean = getRetryCount(record) >= config.maxRetries
}

/**
 * Error handler that delegates to different strategies based on the exception type.
 */
class TypedErrorHandler(
  private val handlers: Map<Class<out Throwable>, (ConsumerRecord<Any, Any>, Throwable) -> Unit>,
  private val defaultHandler: (ConsumerRecord<Any, Any>, Throwable) -> Unit = { record, e ->
    logger.error(e) { "Unhandled error processing record from topic: ${record.topic()}" }
  }
) {
  /**
   * Handles an error by delegating to the appropriate handler.
   */
  fun handle(record: ConsumerRecord<Any, Any>, exception: Throwable) {
    val handler = handlers.entries
      .firstOrNull { (type, _) -> type.isAssignableFrom(exception.javaClass) }
      ?.value
      ?: defaultHandler

    handler(record, exception)
  }
}

/**
 * Builder for TypedErrorHandler.
 */
class TypedErrorHandlerBuilder {
  @PublishedApi
  internal val handlers = mutableMapOf<Class<out Throwable>, (ConsumerRecord<Any, Any>, Throwable) -> Unit>()
  private var defaultHandler: (ConsumerRecord<Any, Any>, Throwable) -> Unit = { record, e ->
    logger.error(e) { "Unhandled error processing record from topic: ${record.topic()}" }
  }

  /**
   * Registers a handler for a specific exception type.
   */
  inline fun <reified T : Throwable> on(noinline handler: (ConsumerRecord<Any, Any>, T) -> Unit) {
    @Suppress("UNCHECKED_CAST")
    handlers[T::class.java] = handler as (ConsumerRecord<Any, Any>, Throwable) -> Unit
  }

  /**
   * Sets the default handler for unmatched exceptions.
   */
  fun default(handler: (ConsumerRecord<Any, Any>, Throwable) -> Unit) {
    defaultHandler = handler
  }

  fun build(): TypedErrorHandler = TypedErrorHandler(handlers, defaultHandler)
}

/**
 * Creates a typed error handler using a DSL.
 */
fun typedErrorHandler(block: TypedErrorHandlerBuilder.() -> Unit): TypedErrorHandler {
  val builder = TypedErrorHandlerBuilder()
  builder.block()
  return builder.build()
}

/**
 * Seek-to-current error handler that reprocesses failed messages.
 */
class SeekToCurrentErrorHandler(
  private val maxFailures: Int = 3,
  private val onMaxFailuresExceeded: (ConsumerRecord<Any, Any>, Exception) -> Unit = { record, e ->
    logger.error(e) { "Max failures exceeded for record from topic: ${record.topic()}" }
  }
) : CommonErrorHandler {
  private val failureCounts = mutableMapOf<TopicPartition, Int>()

  override fun handleRemaining(
    thrownException: Exception,
    records: MutableList<ConsumerRecord<*, *>>,
    consumer: org.apache.kafka.clients.consumer.Consumer<*, *>,
    container: MessageListenerContainer
  ) {
    if (records.isEmpty()) return

    val record = records.first()
    val tp = TopicPartition(record.topic(), record.partition())
    val count = failureCounts.getOrDefault(tp, 0) + 1

    if (count >= maxFailures) {
      failureCounts.remove(tp)
      @Suppress("UNCHECKED_CAST")
      onMaxFailuresExceeded(record as ConsumerRecord<Any, Any>, thrownException)
    } else {
      failureCounts[tp] = count
      consumer.seek(tp, record.offset())
      logger.warn { "Seeking to current offset for $tp, failure count: $count" }
    }
  }

  override fun handleOne(
    thrownException: Exception,
    record: ConsumerRecord<*, *>,
    consumer: org.apache.kafka.clients.consumer.Consumer<*, *>,
    container: MessageListenerContainer
  ): Boolean {
    val tp = TopicPartition(record.topic(), record.partition())
    val count = failureCounts.getOrDefault(tp, 0) + 1

    return if (count >= maxFailures) {
      failureCounts.remove(tp)
      @Suppress("UNCHECKED_CAST")
      onMaxFailuresExceeded(record as ConsumerRecord<Any, Any>, thrownException)
      true // Don't seek
    } else {
      failureCounts[tp] = count
      false // Seek to current
    }
  }
}
