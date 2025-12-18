package io.github.osoykan.kafkaflow

import kotlin.reflect.KClass
import kotlin.time.Duration.Companion.milliseconds

/**
 * Annotation to bind a consumer to one or more topics with full retry configuration.
 * ALL policies are configurable per consumer!
 *
 * Example (single topic):
 * ```kotlin
 * @KafkaTopic(
 *     name = "orders.created",
 *     retry = "orders.created.retry",
 *     dlt = "orders.created.dlt",
 *     concurrency = 4,
 *     maxInMemoryRetries = 3
 * )
 * class OrderCreatedConsumer : ConsumerAutoAck<String, OrderEvent> { ... }
 * ```
 *
 * Example (multiple topics):
 * ```kotlin
 * @KafkaTopic(
 *     topics = ["orders.created", "orders.updated"],
 *     concurrency = 8
 * )
 * class OrderEventsConsumer : ConsumerAutoAck<String, OrderEvent> { ... }
 * ```
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KafkaTopic(
  /** Main topic name (for single topic, use this OR topics) */
  val name: String = "",
  /** Multiple topic names (use this OR name for single topic) */
  val topics: Array<String> = [],
  /** Retry topic name (empty = auto-generate as {name}.retry) */
  val retry: String = "",
  /** Dead letter topic name (empty = auto-generate as {name}.dlt) */
  val dlt: String = "",
  /** Consumer group ID (empty = use default from config) */
  val groupId: String = "",
  /** Number of concurrent record processors (processing concurrency) */
  val concurrency: Int = 1,
  /** Number of Kafka consumer threads/partitions (container concurrency) */
  val multiplePartitions: Int = 1,
  /**
   * Commit batch size (-1 = use ListenerConfig default).
   * - Set only commitSize for BySize strategy
   * - Set both commitSize and commitIntervalMs for BySizeOrTime strategy
   * - Set only commitIntervalMs for ByTime strategy
   */
  val commitSize: Int = -1,
  /** Commit interval in milliseconds (-1 = use ListenerConfig default) */
  val commitIntervalMs: Long = -1,
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
 * Created by TopicResolver from @KafkaTopic annotation or manual config.
 */
data class ResolvedConsumerConfig(
  val topic: TopicConfig,
  val retry: RetryPolicy,
  val classifier: ExceptionClassifier,
  val consumerName: String
) {
  /**
   * Effective retry topic name.
   * Auto-generation (suffix) only works for single-topic consumers.
   * For multi-topic consumers, an explicit retryTopic name must be provided.
   */
  val retryTopic: String
    get() = topic.retryTopic ?: if (topic.topics.size == 1) {
      topic.topics.first() + retry.retryTopicSuffix
    } else {
      error("Consumer '$consumerName' listens to multiple topics ${topic.topics}, so you must provide an explicit 'retryTopic' name.")
    }

  /**
   * Effective DLT topic name.
   * Auto-generation (suffix) only works for single-topic consumers.
   * For multi-topic consumers, an explicit dltTopic name must be provided.
   */
  val dltTopic: String
    get() = topic.dltTopic ?: if (topic.topics.size == 1) {
      topic.topics.first() + retry.dltSuffix
    } else {
      error("Consumer '$consumerName' listens to multiple topics ${topic.topics}, so you must provide an explicit 'dltTopic' name.")
    }
}

/**
 * Resolves topic + retry configuration for consumers.
 */
interface TopicResolver {
  /**
   * Resolves the configuration for a consumer.
   *
   * @param consumer The consumer to resolve config for
   * @return Resolved configuration
   */
  fun resolve(consumer: Consumer<*, *>): ResolvedConsumerConfig
}

/**
 * Default implementation of TopicResolver that supports @KafkaTopic annotations
 * and manual configuration overrides with field-level merging.
 *
 * Resolution priority:
 * 1. Values from manual config (topicConfigs map) override anything else.
 * 2. Values from @KafkaTopic annotation.
 * 3. System defaults.
 */
class DefaultTopicResolver(
  private val topicConfigs: Map<String, TopicConfig> = emptyMap(),
  private val defaultRetryPolicy: RetryPolicy = RetryPolicy.DEFAULT
) : TopicResolver {
  override fun resolve(consumer: Consumer<*, *>): ResolvedConsumerConfig {
    val annotation = consumer::class.java.getAnnotation(KafkaTopic::class.java)
    val manualOverride = topicConfigs[consumer.consumerName]

    if (annotation == null && manualOverride == null) {
      error(
        "No topic config found for consumer: ${consumer.consumerName}. " +
          "Either add @KafkaTopic annotation or provide manual config in topicConfigs map."
      )
    }

    // 1. Resolve base from annotation or defaults
    val base = if (annotation != null) {
      resolveFromAnnotation(annotation, consumer.consumerName)
    } else {
      createBaseFromDefaults(consumer.consumerName)
    }

    // 2. Merge manual override if present
    val resolved = if (manualOverride != null) {
      base.copy(
        topic = base.topic.mergeWith(manualOverride),
        retry = manualOverride.toRetryPolicy(base.retry)
      )
    } else {
      base
    }

    // 3. Final validation
    require(resolved.topic.topics.isNotEmpty()) {
      "No topics configured for consumer: ${consumer.consumerName}. " +
        "Ensure @KafkaTopic specifies 'name'/'topics' or manual config provides them."
    }

    return resolved
  }

  private fun createBaseFromDefaults(consumerName: String): ResolvedConsumerConfig = ResolvedConsumerConfig(
    topic = TopicConfig(topics = emptyList()),
    retry = defaultRetryPolicy,
    classifier = DefaultExceptionClassifier(),
    consumerName = consumerName
  )

  private fun resolveFromAnnotation(
    annotation: KafkaTopic,
    consumerName: String
  ): ResolvedConsumerConfig {
    val additionalNonRetryable = annotation.nonRetryableExceptions
      .map { it.java }
      .toSet()

    val classifier = annotation.classifier.toClassifier(additionalNonRetryable)

    // Resolve topics from either name (single) or topics (multiple)
    val topicNames = when {
      annotation.topics.isNotEmpty() -> annotation.topics.toList()
      annotation.name.isNotBlank() -> listOf(annotation.name)
      else -> emptyList() // Allow empty if we expect manual override to provide it
    }

    val topic = TopicConfig(
      topics = topicNames,
      groupId = annotation.groupId.takeIf { it.isNotBlank() },
      concurrency = annotation.concurrency,
      multiplePartitions = annotation.multiplePartitions,
      retryTopic = annotation.retry.takeIf { it.isNotBlank() },
      dltTopic = annotation.dlt.takeIf { it.isNotBlank() },
      commitSize = annotation.commitSize.takeIf { it > 0 },
      commitIntervalMs = annotation.commitIntervalMs.takeIf { it > 0 }
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
}
