package io.github.osoykan.kafkaflow.poller

import io.github.osoykan.kafkaflow.TopicConfig
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A record with its acknowledgment for commit control.
 *
 * @property record The Kafka consumer record
 * @property acknowledge Function to call after successful processing to commit offset
 */
data class AckableRecord<K, V>(
  val record: ConsumerRecord<K, V>,
  val acknowledge: () -> Unit
)

/**
 * Commit strategy for auto-ack consumers.
 *
 * Determines when offsets are committed for auto-ack consumers.
 * Manual-ack consumers always use immediate commit when `acknowledge()` is called.
 *
 * ## Strategies
 *
 * - [BySize]: Commit every N records (best for throughput)
 * - [ByTime]: Commit every interval (best for latency guarantees)
 * - [BySizeOrTime]: Commit on whichever comes first (balanced)
 *
 * ## Spring Kafka Mapping
 *
 * | Strategy     | Spring Kafka AckMode |
 * |--------------|----------------------|
 * | BySize(1)    | RECORD               |
 * | BySize(n)    | COUNT                |
 * | ByTime       | TIME                 |
 * | BySizeOrTime | COUNT_TIME           |
 */
sealed interface CommitStrategy {
  /**
   * Whether to use synchronous commits.
   *
   * - Per-record (BySize(1)): true - safety over throughput
   * - Batch commits: false - throughput over per-message safety
   */
  val syncCommits: Boolean
    get() = when (this) {
      is BySize -> size == 1

      // Only per-record needs sync commits
      is ByTime -> false

      is BySizeOrTime -> false
    }

  /**
   * Timeout for synchronous commits.
   *
   * Derived from the strategy's timing characteristics.
   */
  val syncCommitTimeout: kotlin.time.Duration
    get() = when (this) {
      is BySize -> kotlin.time.Duration.parse("5s")

      // Default for per-record
      is ByTime -> interval

      // Use the configured interval
      is BySizeOrTime -> interval // Use the configured interval
    }

  /**
   * Commit every [size] records.
   *
   * Use `BySize(1)` for per-record commits (most predictable, lower throughput).
   * Use larger values for better throughput at the cost of potential reprocessing on failure.
   */
  @JvmInline
  value class BySize(
    val size: Int
  ) : CommitStrategy {
    init {
      require(size > 0) { "Commit batch size must be positive, got $size" }
    }
  }

  /**
   * Commit all records every [interval].
   *
   * Good for latency-sensitive consumers where you want predictable commit timing.
   */
  @JvmInline
  value class ByTime(
    val interval: kotlin.time.Duration
  ) : CommitStrategy {
    init {
      require(interval.isPositive() && interval != kotlin.time.Duration.ZERO) {
        "Commit interval must be positive non-zero, got $interval"
      }
    }
  }

  /**
   * Commit every [size] records OR every [interval], whichever comes first.
   *
   * Balanced approach: good throughput with bounded commit latency.
   */
  data class BySizeOrTime(
    val size: Int,
    val interval: kotlin.time.Duration
  ) : CommitStrategy {
    init {
      require(size > 0) { "Commit batch size must be positive, got $size" }
      require(interval.isPositive() && interval != kotlin.time.Duration.ZERO) {
        "Commit interval must be positive non-zero, got $interval"
      }
    }
  }

  companion object {
    /** Per-record commit - most predictable but lower throughput */
    val PerRecord: CommitStrategy = BySize(1)

    /** Default batch commit - good balance of throughput and safety */
    val Default: CommitStrategy = BySizeOrTime(size = 100, interval = kotlin.time.Duration.parse("5s"))
  }
}

/**
 * Default buffer capacity for backpressure.
 */
const val DEFAULT_BUFFER_CAPACITY: Int = Channel.BUFFERED

/**
 * Abstraction for Kafka polling mechanisms.
 *
 * The ack mode is configured at construction time.
 */
interface KafkaPoller<K : Any, V : Any> {
  /**
   * Consumes messages from the specified topic.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control
   * @return Flow of ackable records
   */
  fun poll(
    topic: TopicConfig,
    bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY
  ): Flow<AckableRecord<K, V>>

  /**
   * Stops all active polling.
   */
  fun stop()

  /**
   * Checks if the poller is stopped.
   */
  fun isStopped(): Boolean
}
