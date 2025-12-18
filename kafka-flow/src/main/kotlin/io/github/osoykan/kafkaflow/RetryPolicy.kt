package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate
import kotlin.math.min
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger {}

/**
 * Backoff strategy for retry delays.
 */
sealed class BackoffStrategy {
  /**
   * Calculates the delay for the given attempt number (0-based).
   */
  abstract fun delayFor(attempt: Int): Duration

  /**
   * Fixed delay between retries.
   */
  data class Fixed(
    val delay: Duration = 1.seconds
  ) : BackoffStrategy() {
    override fun delayFor(attempt: Int): Duration = delay
  }

  /**
   * Exponential backoff with configurable parameters.
   *
   * delay = initialDelay * (multiplier ^ attempt), capped at maxDelay
   *
   * @param initialDelay Initial delay for first retry
   * @param multiplier Multiplier for each subsequent retry
   * @param maxDelay Maximum delay cap
   */
  data class Exponential(
    val initialDelay: Duration = 100.milliseconds,
    val multiplier: Double = 2.0,
    val maxDelay: Duration = 30.seconds
  ) : BackoffStrategy() {
    override fun delayFor(attempt: Int): Duration {
      val delayMs = initialDelay.inWholeMilliseconds * multiplier.pow(attempt.toDouble())
      return min(delayMs.toLong(), maxDelay.inWholeMilliseconds).milliseconds
    }
  }

  /**
   * No delay between retries.
   */
  data object None : BackoffStrategy() {
    override fun delayFor(attempt: Int): Duration = Duration.ZERO
  }
}

/**
 * Configuration for retry behavior.
 *
 * The retry flow is:
 * 1. Process message
 * 2. On failure → in-memory retry with backoff (up to [maxInMemoryRetries])
 * 3. After in-memory retries exhausted → send to retry topic
 * 4. Process from retry topic (up to [maxRetryTopicAttempts])
 * 5. After retry topic attempts exhausted → send to DLT
 *
 * TTL support:
 * - [maxRetryDuration]: Maximum total time a message can spend in retry (from first failure)
 * - [maxMessageAge]: Maximum age of a message from its original timestamp
 *
 * @param maxInMemoryRetries Number of in-memory retries before sending to retry topic
 * @param inMemoryBackoff Backoff strategy for in-memory retries
 * @param maxRetryTopicAttempts Number of times to process from retry topic before DLT
 * @param retryTopicBackoff Backoff strategy for retry topic processing
 * @param retryTopicSuffix Suffix for retry topic name
 * @param dltSuffix Suffix for dead letter topic name
 * @param maxRetryDuration Maximum total time in retry before expiring to DLT (null = no limit)
 * @param maxMessageAge Maximum age from original message timestamp before expiring to DLT (null = no limit)
 */
data class RetryPolicy(
  val maxInMemoryRetries: Int = 3,
  val inMemoryBackoff: BackoffStrategy = BackoffStrategy.Exponential(),
  val maxRetryTopicAttempts: Int = 3,
  val retryTopicBackoff: BackoffStrategy = BackoffStrategy.Exponential(
    initialDelay = 1.seconds,
    multiplier = 2.0,
    maxDelay = 60.seconds
  ),
  val retryTopicSuffix: String = ".retry",
  val dltSuffix: String = ".dlt",
  val maxRetryDuration: Duration? = null,
  val maxMessageAge: Duration? = null
) {
  /**
   * Merges another [RetryPolicy] into this one, where the [other] takes priority
   * for non-default values.
   */
  fun mergeWith(other: RetryPolicy): RetryPolicy = copy(
    maxInMemoryRetries = other.maxInMemoryRetries,
    inMemoryBackoff = other.inMemoryBackoff,
    maxRetryTopicAttempts = other.maxRetryTopicAttempts,
    retryTopicBackoff = other.retryTopicBackoff,
    retryTopicSuffix = other.retryTopicSuffix,
    dltSuffix = other.dltSuffix,
    maxRetryDuration = other.maxRetryDuration ?: maxRetryDuration,
    maxMessageAge = other.maxMessageAge ?: maxMessageAge
  )

  /**
   * Updates this policy with non-null values from primitive configuration fields.
   */
  fun updateFrom(
    maxInMemoryRetries: Int? = null,
    maxRetryTopicAttempts: Int? = null,
    maxRetryDurationMs: Long? = null,
    maxMessageAgeMs: Long? = null,
    backoffMs: Long? = null,
    backoffMultiplier: Double? = null,
    maxBackoffMs: Long? = null,
    retryTopicBackoffMs: Long? = null,
    retryTopicBackoffMultiplier: Double? = null,
    maxRetryTopicBackoffMs: Long? = null
  ): RetryPolicy {
    var policy = this.copy(
      maxInMemoryRetries = maxInMemoryRetries ?: this.maxInMemoryRetries,
      maxRetryTopicAttempts = maxRetryTopicAttempts ?: this.maxRetryTopicAttempts,
      maxRetryDuration = maxRetryDurationMs?.milliseconds ?: this.maxRetryDuration,
      maxMessageAge = maxMessageAgeMs?.milliseconds ?: this.maxMessageAge
    )

    // Merge in-memory backoff
    if (backoffMs != null || backoffMultiplier != null || maxBackoffMs != null) {
      val exp = policy.inMemoryBackoff as? BackoffStrategy.Exponential ?: BackoffStrategy.Exponential()
      policy = policy.copy(
        inMemoryBackoff = exp.copy(
          initialDelay = backoffMs?.milliseconds ?: exp.initialDelay,
          multiplier = backoffMultiplier ?: exp.multiplier,
          maxDelay = maxBackoffMs?.milliseconds ?: exp.maxDelay
        )
      )
    }

    // Merge retry topic backoff
    if (retryTopicBackoffMs != null || retryTopicBackoffMultiplier != null || maxRetryTopicBackoffMs != null) {
      val exp = policy.retryTopicBackoff as? BackoffStrategy.Exponential ?: BackoffStrategy.Exponential()
      policy = policy.copy(
        retryTopicBackoff = exp.copy(
          initialDelay = retryTopicBackoffMs?.milliseconds ?: exp.initialDelay,
          multiplier = retryTopicBackoffMultiplier ?: exp.multiplier,
          maxDelay = maxRetryTopicBackoffMs?.milliseconds ?: exp.maxDelay
        )
      )
    }

    return policy
  }

  companion object {
    /**
     * No retries - fail immediately to DLT.
     */
    val NO_RETRY = RetryPolicy(
      maxInMemoryRetries = 0,
      maxRetryTopicAttempts = 0
    )

    /**
     * Default retry policy with exponential backoff.
     */
    val DEFAULT = RetryPolicy()

    /**
     * Aggressive retry policy with more attempts.
     */
    val AGGRESSIVE = RetryPolicy(
      maxInMemoryRetries = 5,
      inMemoryBackoff = BackoffStrategy.Exponential(
        initialDelay = 50.milliseconds,
        multiplier = 1.5,
        maxDelay = 5.seconds
      ),
      maxRetryTopicAttempts = 5
    )

    /**
     * Time-limited retry policy - retries for up to 5 minutes.
     */
    val TIME_LIMITED = RetryPolicy(
      maxRetryDuration = 5.seconds * 60,
      maxMessageAge = 10.seconds * 60
    )
  }
}

/**
 * Result of checking if a message has expired based on TTL settings.
 */
sealed class TtlCheckResult {
  data object NotExpired : TtlCheckResult()

  data class Expired(
    val reason: String
  ) : TtlCheckResult()
}

/**
 * Result of processing a record with retry support.
 */
sealed class ProcessingResult<out T> {
  data class Success<T>(
    val value: T
  ) : ProcessingResult<T>()

  data class SentToRetryTopic(
    val topic: String,
    val attempt: Int
  ) : ProcessingResult<Nothing>()

  data class SentToDlt(
    val topic: String,
    val reason: String,
    val exception: Throwable? = null
  ) : ProcessingResult<Nothing>()

  data class Failed(
    val exception: Throwable,
    val exhaustedRetries: Boolean
  ) : ProcessingResult<Nothing>()

  data class Expired(
    val topic: String,
    val reason: String
  ) : ProcessingResult<Nothing>()
}

/**
 * Result of in-memory retry with metadata.
 */
internal data class InMemoryRetryResult<T>(
  val result: Result<T>,
  val inMemoryRetryCount: Int,
  val processingTimeMs: Long
)

/**
 * Executes a block with in-memory retry support and exception classification.
 *
 * @param policy Retry policy configuration
 * @param classifier Exception classifier for retry decisions
 * @param metrics Metrics for recording retry attempts
 * @param topic Topic name for metrics
 * @param consumerName Consumer name for metrics
 * @param block The block to execute
 * @return Result of the operation with retry metadata
 */
internal suspend fun <T> withRetryInternal(
  policy: RetryPolicy,
  classifier: ExceptionClassifier = AlwaysRetryClassifier,
  metrics: KafkaFlowMetrics = NoOpMetrics,
  topic: String = "",
  consumerName: String = "",
  block: suspend () -> T
): InMemoryRetryResult<T> {
  var lastException: Throwable? = null
  var inMemoryRetryCount = 0
  val startTime = System.currentTimeMillis()

  repeat(policy.maxInMemoryRetries + 1) { attempt ->
    try {
      val result = block()
      return InMemoryRetryResult(
        result = Result.success(result),
        inMemoryRetryCount = inMemoryRetryCount,
        processingTimeMs = System.currentTimeMillis() - startTime
      )
    } catch (e: Throwable) {
      lastException = e

      // Check if exception is retryable
      val category = classifier.classify(e)
      if (category == ExceptionCategory.NonRetryable) {
        logger.debug { "Non-retryable exception: ${e::class.simpleName}, skipping retries" }
        return InMemoryRetryResult(
          result = Result.failure(e),
          inMemoryRetryCount = inMemoryRetryCount,
          processingTimeMs = System.currentTimeMillis() - startTime
        )
      }

      if (attempt < policy.maxInMemoryRetries) {
        inMemoryRetryCount++
        metrics.recordInMemoryRetry(topic, consumerName, inMemoryRetryCount)
        val delayDuration = policy.inMemoryBackoff.delayFor(attempt)
        logger.debug { "Retry attempt ${attempt + 1}/${policy.maxInMemoryRetries}, delaying $delayDuration" }
        delay(delayDuration)
      }
    }
  }

  return InMemoryRetryResult(
    result = Result.failure(lastException ?: IllegalStateException("Retry failed without exception")),
    inMemoryRetryCount = inMemoryRetryCount,
    processingTimeMs = System.currentTimeMillis() - startTime
  )
}

/**
 * Simple withRetry for backward compatibility.
 */
suspend fun <T> withRetry(
  policy: RetryPolicy,
  block: suspend () -> T
): Result<T> = withRetryInternal(policy, block = block).result

/**
 * Processor that handles retries with retry topic and DLT support.
 *
 * INTERNAL: This is used by consumer supervisors. Consumer implementations
 * don't interact with this directly.
 *
 * Features:
 * - In-memory retries with configurable backoff
 * - Retry topic publishing
 * - Dead Letter Topic (DLT) for exhausted retries
 * - Exception classification (retryable vs non-retryable)
 * - TTL support (max retry duration, max message age)
 * - Metrics collection
 * - Enhanced error context in headers
 * - Non-blocking async publishing
 */
class RetryableProcessor<K : Any, V : Any>(
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val policy: RetryPolicy = RetryPolicy.DEFAULT,
  private val classifier: ExceptionClassifier = AlwaysRetryClassifier,
  private val metrics: KafkaFlowMetrics = NoOpMetrics,
  private val consumerName: String = ""
) {
  /**
   * Processes a record with full retry support.
   *
   * Flow:
   * 1. Check TTL - if expired, send to DLT immediately
   * 2. Check exception classification - if non-retryable, send to DLT immediately
   * 3. Execute handler with in-memory retries
   * 4. If still failing → send to retry topic
   * 5. If from retry topic and max attempts reached → send to DLT
   *
   * @param record The record to process
   * @param handler The processing logic
   * @return Processing result
   */
  suspend fun <R> process(
    record: ConsumerRecord<K, V>,
    handler: suspend (ConsumerRecord<K, V>) -> R
  ): ProcessingResult<R> {
    val topic = record.topic()
    val retryTopicAttempt = getRetryTopicAttempt(record)
    val isFromRetryTopic = retryTopicAttempt > 0

    // Check TTL first
    val ttlCheck = checkTtl(record)
    if (ttlCheck is TtlCheckResult.Expired) {
      logger.warn { "Message expired: ${ttlCheck.reason}" }
      metrics.recordExpired(topic, consumerName, ttlCheck.reason)
      return sendToDltForExpiry(record, ttlCheck.reason)
    }

    // Apply retry topic backoff if coming from retry topic
    if (isFromRetryTopic && retryTopicAttempt > 1) {
      val delayDuration = policy.retryTopicBackoff.delayFor(retryTopicAttempt - 1)
      logger.debug { "Retry topic attempt $retryTopicAttempt, delaying $delayDuration" }
      delay(delayDuration)
    }

    // Try with in-memory retries (with classification)
    val retryResult = withRetryInternal(
      policy = policy,
      classifier = classifier,
      metrics = metrics,
      topic = topic,
      consumerName = consumerName
    ) { handler(record) }

    return when {
      retryResult.result.isSuccess -> {
        ProcessingResult.Success(retryResult.result.getOrThrow())
      }

      // Check if non-retryable - go straight to DLT
      classifier.classify(retryResult.result.exceptionOrNull()!!) == ExceptionCategory.NonRetryable -> {
        sendToDlt(record, retryResult.result.exceptionOrNull()!!, retryResult, "Non-retryable exception")
      }

      // Max retry topic attempts reached → DLT
      isFromRetryTopic && retryTopicAttempt >= policy.maxRetryTopicAttempts -> {
        sendToDlt(
          record,
          retryResult.result.exceptionOrNull()!!,
          retryResult,
          "Exhausted ${policy.maxRetryTopicAttempts} retry topic attempts"
        )
      }

      // Send to retry topic
      else -> {
        sendToRetryTopic(record, retryResult.result.exceptionOrNull()!!, retryTopicAttempt, retryResult)
      }
    }
  }

  /**
   * Checks if a message has expired based on TTL settings.
   */
  private fun checkTtl(record: ConsumerRecord<K, V>): TtlCheckResult {
    val now = System.currentTimeMillis()

    // Check max retry duration (from first failure)
    if (policy.maxRetryDuration != null) {
      val firstFailure = record
        .headers()
        .lastHeader(Headers.FIRST_FAILURE_TIME)
        ?.value()
        ?.let { String(it).toLongOrNull() }

      if (firstFailure != null) {
        val retryDuration = now - firstFailure
        if (retryDuration > policy.maxRetryDuration.inWholeMilliseconds) {
          return TtlCheckResult.Expired(
            "Max retry duration exceeded: ${retryDuration}ms > ${policy.maxRetryDuration.inWholeMilliseconds}ms"
          )
        }
      }
    }

    // Check max message age (from original timestamp)
    // Only check if timestamp is valid (>= 0, as ConsumerRecord.NO_TIMESTAMP is -1)
    if (policy.maxMessageAge != null && record.timestamp() >= 0) {
      val messageAge = now - record.timestamp()
      if (messageAge > policy.maxMessageAge.inWholeMilliseconds) {
        return TtlCheckResult.Expired(
          "Max message age exceeded: ${messageAge}ms > ${policy.maxMessageAge.inWholeMilliseconds}ms"
        )
      }
    }

    return TtlCheckResult.NotExpired
  }

  private fun getRetryTopicAttempt(record: ConsumerRecord<K, V>): Int = record
    .headers()
    .lastHeader(Headers.RETRY_TOPIC_ATTEMPT)
    ?.value()
    ?.let { String(it).toIntOrNull() }
    ?: 0

  private fun getInMemoryRetryCount(record: ConsumerRecord<K, V>): Int = record
    .headers()
    .lastHeader(Headers.RETRY_COUNT)
    ?.value()
    ?.let { String(it).toIntOrNull() }
    ?: 0

  private suspend fun sendToRetryTopic(
    record: ConsumerRecord<K, V>,
    exception: Throwable,
    currentAttempt: Int,
    retryResult: InMemoryRetryResult<*>
  ): ProcessingResult<Nothing> {
    val originalTopic = record
      .headers()
      .lastHeader(Headers.ORIGINAL_TOPIC)
      ?.value()
      ?.let { String(it) }
      ?: record.topic()

    val retryTopic = originalTopic + policy.retryTopicSuffix
    val newAttempt = currentAttempt + 1

    try {
      val retryRecord = createRetryRecord(record, retryTopic, originalTopic, newAttempt, exception, retryResult)
      // Non-blocking publish using await()
      kafkaTemplate.send(retryRecord).await()
      metrics.recordSentToRetryTopic(originalTopic, retryTopic, newAttempt)
      logger.info { "Sent to retry topic: $retryTopic (attempt $newAttempt)" }
      return ProcessingResult.SentToRetryTopic(retryTopic, newAttempt)
    } catch (e: Exception) {
      logger.error(e) { "Failed to send to retry topic: $retryTopic" }
      metrics.recordProcessingFailure(originalTopic, consumerName, e)
      return ProcessingResult.Failed(e, exhaustedRetries = false)
    }
  }

  private suspend fun sendToDlt(
    record: ConsumerRecord<K, V>,
    exception: Throwable,
    retryResult: InMemoryRetryResult<*>,
    reason: String
  ): ProcessingResult<Nothing> {
    val originalTopic = record
      .headers()
      .lastHeader(Headers.ORIGINAL_TOPIC)
      ?.value()
      ?.let { String(it) }
      ?: record.topic().removeSuffix(policy.retryTopicSuffix)

    val dltTopic = originalTopic + policy.dltSuffix
    val retryTopicAttempt = getRetryTopicAttempt(record)
    val totalRetries = retryResult.inMemoryRetryCount + (retryTopicAttempt * policy.maxInMemoryRetries)

    try {
      val dltRecord = createDltRecord(record, dltTopic, originalTopic, exception, retryResult, totalRetries)
      // Non-blocking publish using await()
      kafkaTemplate.send(dltRecord).await()
      metrics.recordSentToDlt(originalTopic, dltTopic, totalRetries)
      logger.warn { "Sent to DLT: $dltTopic - $reason" }
      return ProcessingResult.SentToDlt(dltTopic, reason, exception)
    } catch (e: Exception) {
      logger.error(e) { "Failed to send to DLT: $dltTopic" }
      metrics.recordProcessingFailure(originalTopic, consumerName, e)
      return ProcessingResult.Failed(e, exhaustedRetries = true)
    }
  }

  private suspend fun sendToDltForExpiry(
    record: ConsumerRecord<K, V>,
    reason: String
  ): ProcessingResult<Nothing> {
    val originalTopic = record
      .headers()
      .lastHeader(Headers.ORIGINAL_TOPIC)
      ?.value()
      ?.let { String(it) }
      ?: record.topic().removeSuffix(policy.retryTopicSuffix)

    val dltTopic = originalTopic + policy.dltSuffix
    val retryTopicAttempt = getRetryTopicAttempt(record)
    val inMemoryCount = getInMemoryRetryCount(record)
    val totalRetries = inMemoryCount + (retryTopicAttempt * policy.maxInMemoryRetries)

    try {
      val dltRecord = ProducerRecord<K, V>(dltTopic, record.key(), record.value()).apply {
        // Copy all headers
        record.headers().forEach { headers().add(it) }

        // Add expiry metadata
        headers().add(RecordHeader(Headers.ORIGINAL_TOPIC, originalTopic.toByteArray()))
        headers().add(RecordHeader(Headers.LAST_FAILURE_TIME, System.currentTimeMillis().toString().toByteArray()))
        headers().add(RecordHeader(Headers.TOTAL_RETRY_COUNT, totalRetries.toString().toByteArray()))
        headers().add(RecordHeader(Headers.CONSUMER_NAME, consumerName.toByteArray()))
      }

      kafkaTemplate.send(dltRecord).await()
      metrics.recordSentToDlt(originalTopic, dltTopic, totalRetries)
      logger.warn { "Sent expired message to DLT: $dltTopic - $reason" }
      return ProcessingResult.Expired(dltTopic, reason)
    } catch (e: Exception) {
      logger.error(e) { "Failed to send expired message to DLT: $dltTopic" }
      return ProcessingResult.Failed(e, exhaustedRetries = true)
    }
  }

  private fun createRetryRecord(
    record: ConsumerRecord<K, V>,
    retryTopic: String,
    originalTopic: String,
    attempt: Int,
    exception: Throwable,
    retryResult: InMemoryRetryResult<*>
  ): ProducerRecord<K, V> = ProducerRecord<K, V>(retryTopic, record.key(), record.value()).apply {
    // Copy original headers (except retry-related ones)
    record
      .headers()
      .filter { !it.key().startsWith("x-") && !it.key().startsWith("kafka.") }
      .forEach { headers().add(it) }

    // Add retry metadata
    headers().add(RecordHeader(Headers.ORIGINAL_TOPIC, originalTopic.toByteArray()))
    headers().add(RecordHeader(Headers.RETRY_TOPIC_ATTEMPT, attempt.toString().toByteArray()))
    headers().add(RecordHeader(Headers.RETRY_COUNT, retryResult.inMemoryRetryCount.toString().toByteArray()))
    headers().add(RecordHeader(Headers.LAST_FAILURE_TIME, System.currentTimeMillis().toString().toByteArray()))

    // Add enhanced error context
    val errorContext = EnrichedErrorContext.from(
      exception = exception,
      processingTimeMs = retryResult.processingTimeMs,
      attemptNumber = attempt,
      inMemoryRetryCount = retryResult.inMemoryRetryCount,
      retryTopicAttempt = attempt,
      consumerName = consumerName
    )
    errorContext.addHeaders(this)

    // Preserve first failure time
    val firstFailureTime = record
      .headers()
      .lastHeader(Headers.FIRST_FAILURE_TIME)
      ?.value()
      ?.let { String(it) }
      ?: System.currentTimeMillis().toString()
    headers().add(RecordHeader(Headers.FIRST_FAILURE_TIME, firstFailureTime.toByteArray()))
  }

  private fun createDltRecord(
    record: ConsumerRecord<K, V>,
    dltTopic: String,
    originalTopic: String,
    exception: Throwable,
    retryResult: InMemoryRetryResult<*>,
    totalRetries: Int
  ): ProducerRecord<K, V> = ProducerRecord<K, V>(dltTopic, record.key(), record.value()).apply {
    // Copy all headers
    record.headers().forEach { headers().add(it) }

    // Add/update DLT metadata
    headers().add(RecordHeader(Headers.ORIGINAL_TOPIC, originalTopic.toByteArray()))
    headers().add(RecordHeader(Headers.LAST_FAILURE_TIME, System.currentTimeMillis().toString().toByteArray()))

    // Add enhanced error context
    val errorContext = EnrichedErrorContext.from(
      exception = exception,
      processingTimeMs = retryResult.processingTimeMs,
      totalRetries = totalRetries,
      inMemoryRetryCount = retryResult.inMemoryRetryCount,
      retryTopicAttempt = getRetryTopicAttempt(record),
      consumerName = consumerName
    )
    errorContext.addHeaders(this)
  }
}

/**
 * Extension function to process records with retry support.
 */
suspend fun <K : Any, V : Any, R> ConsumerRecord<K, V>.processWithRetry(
  kafkaTemplate: KafkaTemplate<K, V>,
  policy: RetryPolicy = RetryPolicy.DEFAULT,
  classifier: ExceptionClassifier = AlwaysRetryClassifier,
  metrics: KafkaFlowMetrics = NoOpMetrics,
  consumerName: String = "",
  handler: suspend (ConsumerRecord<K, V>) -> R
): ProcessingResult<R> = RetryableProcessor(kafkaTemplate, policy, classifier, metrics, consumerName).process(this, handler)
