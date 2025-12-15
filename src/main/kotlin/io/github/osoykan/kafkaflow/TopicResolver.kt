package io.github.osoykan.kafkaflow

import kotlin.reflect.KClass
import kotlin.time.Duration.Companion.milliseconds

/**
 * Annotation to bind a consumer to a topic with full retry configuration.
 * ALL policies are configurable per consumer!
 *
 * Example:
 * ```kotlin
 * @KafkaTopic(
 *     name = "orders.created",
 *     retry = "orders.created.retry",
 *     dlt = "orders.created.dlt",
 *     concurrency = 4,
 *     maxInMemoryRetries = 3,
 *     maxRetryTopicAttempts = 2,
 *     backoffMs = 100,
 *     backoffMultiplier = 2.0,
 *     maxRetryDurationMs = 300000,  // 5 minutes max
 *     classifier = ClassifierType.DEFAULT
 * )
 * class OrderCreatedConsumer : ConsumerAutoAck<String, OrderEvent> { ... }
 * ```
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KafkaTopic(
  /** Main topic name */
  val name: String,
  /** Retry topic name (empty = auto-generate as {name}.retry) */
  val retry: String = "",
  /** Dead letter topic name (empty = auto-generate as {name}.dlt) */
  val dlt: String = "",
  /** Consumer group ID (empty = use default from config) */
  val groupId: String = "",
  /** Number of concurrent consumers for this topic */
  val concurrency: Int = 1,
  /** Maximum number of in-memory retries before sending to retry topic */
  val maxInMemoryRetries: Int = 3,
  /** Initial backoff delay in milliseconds for in-memory retries */
  val backoffMs: Long = 100,
  /** Backoff multiplier for exponential backoff */
  val backoffMultiplier: Double = 2.0,
  /** Maximum backoff delay in milliseconds */
  val maxBackoffMs: Long = 30_000,
  /** Maximum number of retry topic attempts before sending to DLT */
  val maxRetryTopicAttempts: Int = 3,
  /** Initial backoff delay in milliseconds for retry topic processing */
  val retryTopicBackoffMs: Long = 1_000,
  /** Backoff multiplier for retry topic exponential backoff */
  val retryTopicBackoffMultiplier: Double = 2.0,
  /** Maximum backoff delay in milliseconds for retry topic */
  val maxRetryTopicBackoffMs: Long = 60_000,
  /** Maximum total retry duration in milliseconds (-1 = no limit) */
  val maxRetryDurationMs: Long = -1,
  /** Maximum message age in milliseconds from original timestamp (-1 = no limit) */
  val maxMessageAgeMs: Long = -1,
  /** Predefined exception classifier type */
  val classifier: ClassifierType = ClassifierType.DEFAULT,
  /** Additional non-retryable exceptions (beyond what classifier defines) */
  val nonRetryableExceptions: Array<KClass<out Throwable>> = []
)

/**
 * Resolved configuration for a consumer.
 * Created by TopicResolver from @KafkaTopic annotation or config map.
 */
data class ResolvedConsumerConfig(
  val topic: TopicConfig,
  val retry: RetryPolicy,
  val classifier: ExceptionClassifier,
  val consumerName: String
) {
  /**
   * Effective retry topic name.
   */
  val retryTopic: String?
    get() = topic.retryTopic ?: topic.name + retry.retryTopicSuffix

  /**
   * Effective DLT topic name.
   */
  val dltTopic: String
    get() = topic.dltTopic ?: topic.name + retry.dltSuffix
}

/**
 * Resolves topic + retry configuration for consumers.
 * Uses @KafkaTopic annotation if present, otherwise falls back to config map.
 */
class TopicResolver(
  private val topicConfigs: Map<String, TopicConfig> = emptyMap(),
  private val defaultGroupId: String = "",
  private val defaultRetryPolicy: RetryPolicy = RetryPolicy.DEFAULT
) {
  /**
   * Resolves the configuration for a consumer.
   *
   * @param consumer The consumer to resolve config for
   * @return Resolved configuration
   * @throws IllegalStateException if no config found for consumer
   */
  fun resolve(consumer: Consumer<*, *>): ResolvedConsumerConfig {
    val annotation = consumer::class.java.getAnnotation(KafkaTopic::class.java)

    return if (annotation != null) {
      resolveFromAnnotation(annotation, consumer.consumerName)
    } else {
      resolveFromConfig(consumer.consumerName)
    }
  }

  private fun resolveFromAnnotation(
    annotation: KafkaTopic,
    consumerName: String
  ): ResolvedConsumerConfig {
    val additionalNonRetryable = annotation.nonRetryableExceptions
      .map { it.java }
      .toSet()

    val classifier = annotation.classifier.toClassifier(additionalNonRetryable)

    val topic = TopicConfig(
      name = annotation.name,
      concurrency = annotation.concurrency,
      retryTopic = annotation.retry.takeIf { it.isNotBlank() },
      dltTopic = annotation.dlt.takeIf { it.isNotBlank() }
    )

    val retryPolicy = RetryPolicy(
      maxInMemoryRetries = annotation.maxInMemoryRetries,
      inMemoryBackoff = BackoffStrategy.Exponential(
        initialDelay = annotation.backoffMs.milliseconds,
        multiplier = annotation.backoffMultiplier,
        maxDelay = annotation.maxBackoffMs.milliseconds
      ),
      maxRetryTopicAttempts = annotation.maxRetryTopicAttempts,
      retryTopicBackoff = BackoffStrategy.Exponential(
        initialDelay = annotation.retryTopicBackoffMs.milliseconds,
        multiplier = annotation.retryTopicBackoffMultiplier,
        maxDelay = annotation.maxRetryTopicBackoffMs.milliseconds
      ),
      maxRetryDuration = annotation.maxRetryDurationMs
        .takeIf { it > 0 }
        ?.milliseconds,
      maxMessageAge = annotation.maxMessageAgeMs
        .takeIf { it > 0 }
        ?.milliseconds
    )

    return ResolvedConsumerConfig(
      topic = topic,
      retry = retryPolicy,
      classifier = classifier,
      consumerName = consumerName
    )
  }

  private fun resolveFromConfig(consumerName: String): ResolvedConsumerConfig {
    val topicConfig = topicConfigs[consumerName]
      ?: error(
        "No topic config found for consumer: $consumerName. " +
          "Either add @KafkaTopic annotation or provide config for '$consumerName'"
      )

    return ResolvedConsumerConfig(
      topic = topicConfig,
      retry = defaultRetryPolicy,
      classifier = DefaultExceptionClassifier(),
      consumerName = consumerName
    )
  }
}
