package io.github.osoykan.kafkaflow

import org.springframework.kafka.listener.ContainerProperties
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
 * Kafka listener container configuration.
 *
 * @property concurrency Number of concurrent consumer threads
 * @property pollTimeout Poll timeout duration
 * @property ackMode Acknowledgment mode
 * @property idleBetweenPolls Idle time between polls
 * @property syncCommits Whether to use synchronous commits
 * @property syncCommitTimeout Synchronous commit timeout
 * @property useVirtualThreads Enable JDK 21+ virtual threads for Kafka polling (default: true)
 *                              Virtual threads are ideal for the blocking consumer.poll() operation.
 *                              This does NOT affect Kotlin coroutines - they continue using Dispatchers.IO.
 */
data class ListenerConfig(
  val concurrency: Int = 4,
  val pollTimeout: Duration = 1.seconds,
  val ackMode: AckMode = AckMode.MANUAL_IMMEDIATE,
  val idleBetweenPolls: Duration = Duration.ZERO,
  val syncCommits: Boolean = true,
  val syncCommitTimeout: Duration = 5.seconds,
  val useVirtualThreads: Boolean = true
)

/**
 * Topic-specific configuration that can override listener defaults.
 *
 * @property name Topic name
 * @property concurrency Override default concurrency for this topic
 * @property pollTimeout Override default poll timeout for this topic
 * @property retryTopic Optional retry topic name
 * @property dltTopic Optional dead letter topic name
 * @property maxRetries Maximum retry attempts before sending to DLT
 * @property retryBackoff Backoff duration between retries
 */
data class TopicConfig(
  val name: String,
  val concurrency: Int? = null,
  val pollTimeout: Duration? = null,
  val retryTopic: String? = null,
  val dltTopic: String? = null,
  val maxRetries: Int = 3,
  val retryBackoff: Duration = 1.seconds
) {
  /**
   * Gets the effective concurrency, using topic-specific or default.
   */
  fun effectiveConcurrency(default: Int): Int = concurrency ?: default

  /**
   * Gets the effective poll timeout, using topic-specific or default.
   */
  fun effectivePollTimeout(default: Duration): Duration = pollTimeout ?: default
}

/**
 * Acknowledgment mode for message processing.
 */
enum class AckMode {
  /**
   * Commit after each record is processed.
   */
  RECORD,

  /**
   * Commit after all records from poll are processed.
   */
  BATCH,

  /**
   * Commit after listener returns without error.
   */
  TIME,

  /**
   * Commit after specified count of records.
   */
  COUNT,

  /**
   * Commit after count or time, whichever comes first.
   */
  COUNT_TIME,

  /**
   * Commit immediately when acknowledgment is called.
   */
  MANUAL_IMMEDIATE,

  /**
   * Commit when all records have been acknowledged.
   */
  MANUAL;

  /**
   * Converts to Spring Kafka ContainerProperties.AckMode.
   */
  fun toSpringAckMode(): ContainerProperties.AckMode = when (this) {
    RECORD -> ContainerProperties.AckMode.RECORD
    BATCH -> ContainerProperties.AckMode.BATCH
    TIME -> ContainerProperties.AckMode.TIME
    COUNT -> ContainerProperties.AckMode.COUNT
    COUNT_TIME -> ContainerProperties.AckMode.COUNT_TIME
    MANUAL_IMMEDIATE -> ContainerProperties.AckMode.MANUAL_IMMEDIATE
    MANUAL -> ContainerProperties.AckMode.MANUAL
  }
}
