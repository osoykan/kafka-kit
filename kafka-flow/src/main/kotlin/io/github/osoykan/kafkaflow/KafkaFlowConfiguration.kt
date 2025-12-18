package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.CommitStrategy
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Main configuration for Kafka Flow consumers and producers.
 *
 * @property bootstrapServers Kafka bootstrap servers (comma-separated)
 * @property producer Producer configuration
 * @property consumer Consumer configuration
 * @property listener Listener container configuration
 */
data class KafkaFlowConfig(
  val bootstrapServers: String,
  val producer: ProducerConfig = ProducerConfig(),
  val consumer: ConsumerConfig,
  val listener: ListenerConfig = ListenerConfig()
)

/**
 * Kafka producer configuration.
 *
 * @property acks Acknowledgment mode (all, 1, 0)
 * @property retries Number of retries
 * @property batchSize Batch size in bytes
 * @property lingerMs Linger time in milliseconds
 * @property bufferMemory Buffer memory in bytes
 * @property compressionType Compression type (none, gzip, snappy, lz4, zstd)
 * @property idempotence Enable idempotent producer
 * @property maxInFlightRequestsPerConnection Maximum in-flight requests per connection
 * @property deliveryTimeout Delivery timeout
 * @property requestTimeout Request timeout
 */
data class ProducerConfig(
  val acks: String = "all",
  val retries: Int = 3,
  val batchSize: Int = 16384,
  val lingerMs: Long = 5,
  val bufferMemory: Long = 33554432,
  val compressionType: String = "lz4",
  val idempotence: Boolean = true,
  val maxInFlightRequestsPerConnection: Int = 5,
  val deliveryTimeout: Duration = 120.seconds,
  val requestTimeout: Duration = 30.seconds
)

/**
 * Kafka consumer configuration.
 *
 * @property groupId Consumer group ID
 * @property autoOffsetReset Auto offset reset strategy (earliest, latest, none)
 * @property enableAutoCommit Enable auto commit
 * @property maxPollRecords Maximum records per poll
 * @property maxPollInterval Maximum poll interval
 * @property sessionTimeout Session timeout
 * @property heartbeatInterval Heartbeat interval
 * @property fetchMinBytes Minimum fetch bytes
 * @property fetchMaxWait Maximum fetch wait time
 * @property isolationLevel Isolation level (read_uncommitted, read_committed)
 * @property partitionAssignmentStrategy Partition assignment strategy
 */
data class ConsumerConfig(
  val groupId: String,
  val autoOffsetReset: String = "earliest",
  val enableAutoCommit: Boolean = false,
  val maxPollRecords: Int = 500,
  val maxPollInterval: Duration = 5.seconds * 60,
  val sessionTimeout: Duration = 45.seconds,
  val heartbeatInterval: Duration = 3.seconds,
  val fetchMinBytes: Int = 1,
  val fetchMaxWait: Duration = 500.milliseconds,
  val isolationLevel: String = "read_committed",
  val partitionAssignmentStrategy: String = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
)

/**
 * Backpressure configuration for flow-based consumers.
 *
 * When processing is slow and the buffer fills up, the container will be paused
 * to prevent exceeding max.poll.interval.ms and causing consumer rebalances.
 *
 * @property enabled Whether backpressure handling is enabled
 * @property pauseThreshold Pause container when buffer fill level exceeds this ratio (0.0-1.0)
 * @property resumeThreshold Resume container when buffer fill level drops below this ratio (0.0-1.0)
 */
data class BackpressureConfig(
  val enabled: Boolean = true,
  val pauseThreshold: Double = 0.8,
  val resumeThreshold: Double = 0.5
) {
  init {
    require(pauseThreshold in 0.0..1.0) { "pauseThreshold must be between 0.0 and 1.0" }
    require(resumeThreshold in 0.0..1.0) { "resumeThreshold must be between 0.0 and 1.0" }
    require(resumeThreshold < pauseThreshold) { "resumeThreshold must be less than pauseThreshold" }
  }
}

/**
 * Kafka listener container configuration.
 *
 * @property concurrency Number of concurrent record processors (processing concurrency).
 *   This controls how many records are processed in parallel from the flow.
 * @property multiplePartitions Number of Kafka consumer threads/partitions (container concurrency).
 *   Set to > 1 when consuming from topics with multiple partitions.
 *   Each partition consumer will process records with the configured [concurrency].
 * @property pollTimeout Poll timeout duration
 * @property commitStrategy Commit strategy for auto-ack consumers (default: PerRecord).
 *   Also determines syncCommits and syncCommitTimeout.
 * @property idleBetweenPolls Idle time between polls
 * @property backpressure Backpressure configuration for pause/resume behavior
 */
data class ListenerConfig(
  val concurrency: Int = 4,
  val multiplePartitions: Int = 1,
  val pollTimeout: Duration = 1.seconds,
  val commitStrategy: CommitStrategy = CommitStrategy.PerRecord,
  val idleBetweenPolls: Duration = Duration.ZERO,
  val backpressure: BackpressureConfig = BackpressureConfig()
)

/**
 * Topic-specific configuration that can override listener defaults.
 * Supports subscribing to one or multiple topics.
 *
 * @property topics List of topic names to subscribe to
 * @property groupId Override default consumer group ID
 * @property concurrency Override default processing concurrency for this topic.
 *   Controls how many records from this topic are processed in parallel.
 * @property multiplePartitions Override default partition consumers for this topic.
 *   Controls how many Kafka consumer threads/partitions are used.
 * @property pollTimeout Override default poll timeout for this topic
 * @property retryTopic Optional retry topic name
 * @property dltTopic Optional dead letter topic name
 * @property maxRetryTopicAttempts Maximum number of retry topic attempts before sending to DLT
 * @property retryTopicBackoffMs Initial backoff delay in milliseconds for retry topic processing
 * @property retryTopicBackoffMultiplier Multiplier for retry topic exponential backoff
 * @property maxRetryTopicBackoffMs Maximum backoff delay in milliseconds for retry topic
 * @property maxInMemoryRetries Maximum number of in-memory retries before sending to retry topic
 * @property backoffMs Initial backoff delay in milliseconds for in-memory retries
 * @property backoffMultiplier Multiplier for in-memory exponential backoff
 * @property maxBackoffMs Maximum backoff delay in milliseconds for in-memory retries
 * @property maxRetryDurationMs Maximum total retry duration in milliseconds
 * @property maxMessageAgeMs Maximum message age in milliseconds from original timestamp
 */
data class TopicConfig(
  val topics: List<String> = emptyList(),
  val groupId: String? = null,
  val concurrency: Int? = null,
  val multiplePartitions: Int? = null,
  val pollTimeout: Duration? = null,
  val retryTopic: String? = null,
  val dltTopic: String? = null,
  // Retry Topic settings
  val maxRetryTopicAttempts: Int? = null,
  val retryTopicBackoffMs: Long? = null,
  val retryTopicBackoffMultiplier: Double? = null,
  val maxRetryTopicBackoffMs: Long? = null,
  // In-memory retry settings
  val maxInMemoryRetries: Int? = null,
  val backoffMs: Long? = null,
  val backoffMultiplier: Double? = null,
  val maxBackoffMs: Long? = null,
  // TTL settings
  val maxRetryDurationMs: Long? = null,
  val maxMessageAgeMs: Long? = null
) {
  init {
    // Only require topics if we're not just doing a partial override
    // (though in practice topics should usually be provided)
  }

  /**
   * Primary constructor for single topic.
   */
  constructor(
    name: String,
    concurrency: Int? = null,
    multiplePartitions: Int? = null,
    pollTimeout: Duration? = null,
    retryTopic: String? = null,
    dltTopic: String? = null,
    maxRetries: Int = 3,
    retryBackoff: Duration = 1.seconds
  ) : this(
    topics = listOf(name),
    concurrency = concurrency,
    multiplePartitions = multiplePartitions,
    pollTimeout = pollTimeout,
    retryTopic = retryTopic,
    dltTopic = dltTopic,
    maxInMemoryRetries = maxRetries,
    backoffMs = retryBackoff.inWholeMilliseconds
  )

  /**
   * Gets the single topic name (for backward compatibility).
   * Throws if multiple topics are configured.
   */
  val name: String
    get() = if (topics.size == 1) {
      topics.first()
    } else {
      error("Multiple topics configured. Use 'topics' property instead.")
    }

  /**
   * Gets a display name for logging (comma-separated topics).
   */
  val displayName: String
    get() = if (topics.size == 1) topics.first() else topics.joinToString(",")

  /**
   * Gets the effective concurrency, using topic-specific or default.
   */
  fun effectiveConcurrency(default: Int): Int = concurrency ?: default

  /**
   * Gets the effective partition consumers, using topic-specific or default.
   */
  fun effectiveMultiplePartitions(default: Int): Int = multiplePartitions ?: default

  /**
   * Gets the effective poll timeout, using topic-specific or default.
   */
  fun effectivePollTimeout(default: Duration): Duration = pollTimeout ?: default

  /**
   * Merges another [TopicConfig] into this one. Fields in the [override] take priority
   * if they are not null (or not empty for lists).
   */
  fun mergeWith(override: TopicConfig): TopicConfig = copy(
    topics = if (override.topics.isNotEmpty()) override.topics else topics,
    groupId = override.groupId ?: groupId,
    concurrency = override.concurrency ?: concurrency,
    multiplePartitions = override.multiplePartitions ?: multiplePartitions,
    pollTimeout = override.pollTimeout ?: pollTimeout,
    retryTopic = override.retryTopic ?: retryTopic,
    dltTopic = override.dltTopic ?: dltTopic,
    maxRetryTopicAttempts = override.maxRetryTopicAttempts ?: maxRetryTopicAttempts,
    retryTopicBackoffMs = override.retryTopicBackoffMs ?: retryTopicBackoffMs,
    retryTopicBackoffMultiplier = override.retryTopicBackoffMultiplier ?: retryTopicBackoffMultiplier,
    maxRetryTopicBackoffMs = override.maxRetryTopicBackoffMs ?: maxRetryTopicBackoffMs,
    maxInMemoryRetries = override.maxInMemoryRetries ?: maxInMemoryRetries,
    backoffMs = override.backoffMs ?: backoffMs,
    backoffMultiplier = override.backoffMultiplier ?: backoffMultiplier,
    maxBackoffMs = override.maxBackoffMs ?: maxBackoffMs,
    maxRetryDurationMs = override.maxRetryDurationMs ?: maxRetryDurationMs,
    maxMessageAgeMs = override.maxMessageAgeMs ?: maxMessageAgeMs
  )

  /**
   * Converts this [TopicConfig] to a [RetryPolicy] using the provided [base] as a template.
   */
  fun toRetryPolicy(base: RetryPolicy): RetryPolicy = base.updateFrom(
    maxInMemoryRetries = maxInMemoryRetries,
    maxRetryTopicAttempts = maxRetryTopicAttempts,
    maxRetryDurationMs = maxRetryDurationMs,
    maxMessageAgeMs = maxMessageAgeMs,
    backoffMs = backoffMs,
    backoffMultiplier = backoffMultiplier,
    maxBackoffMs = maxBackoffMs,
    retryTopicBackoffMs = retryTopicBackoffMs,
    retryTopicBackoffMultiplier = retryTopicBackoffMultiplier,
    maxRetryTopicBackoffMs = maxRetryTopicBackoffMs
  )

  companion object {
    /**
     * Creates a TopicConfig for multiple topics.
     */
    fun of(vararg topics: String): TopicConfig = TopicConfig(topics = topics.toList())

    /**
     * Creates a TopicConfig for multiple topics with configuration.
     */
    fun of(
      topics: List<String>,
      concurrency: Int? = null,
      multiplePartitions: Int? = null,
      pollTimeout: Duration? = null
    ): TopicConfig = TopicConfig(
      topics = topics,
      concurrency = concurrency,
      multiplePartitions = multiplePartitions,
      pollTimeout = pollTimeout
    )
  }
}
