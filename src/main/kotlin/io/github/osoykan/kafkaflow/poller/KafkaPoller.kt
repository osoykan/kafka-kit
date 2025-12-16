package io.github.osoykan.kafkaflow.poller

import io.github.osoykan.kafkaflow.TopicConfig
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A record with its acknowledgment for manual commit control.
 *
 * @property record The Kafka consumer record
 * @property acknowledge Function to call after successful processing
 */
data class AckableRecord<K, V>(
  val record: ConsumerRecord<K, V>,
  val acknowledge: () -> Unit
)

/**
 * Commit strategy for auto-ack consumers.
 *
 * Inspired by [kotlin-kafka](https://github.com/nomisRev/kotlin-kafka).
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
   * Commit every [size] acknowledged records.
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
   * Commit all acknowledged records every [interval].
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
 *
 * This controls how many records can be buffered before the producer
 * (Kafka listener) is blocked, providing backpressure.
 *
 * - [Channel.RENDEZVOUS] (0): No buffering, immediate backpressure
 * - Small values (1-10): Tight backpressure, lower memory
 * - [Channel.BUFFERED] (64): Default, balanced throughput/memory
 * - Large values (100+): High throughput, more memory usage
 * - [Channel.UNLIMITED]: No backpressure (not recommended)
 */
const val DEFAULT_BUFFER_CAPACITY: Int = Channel.BUFFERED

/**
 * Abstraction for Kafka polling mechanisms.
 *
 * Uses Spring Kafka's ConcurrentMessageListenerContainer under the hood,
 * providing a Flow-based API for consuming records.
 *
 * ## Backpressure
 *
 * The poller uses a bounded channel to provide backpressure. When the buffer
 * is full, the Kafka listener thread blocks until the consumer catches up.
 * This prevents unbounded memory growth when consumers are slower than producers.
 *
 * Configure buffer capacity to tune the trade-off:
 * - Smaller buffer = More responsive backpressure, lower memory
 * - Larger buffer = Better throughput, higher memory usage
 *
 * ## Commit Behavior
 *
 * The poller provides two consumption modes:
 *
 * ### Auto-ack (`poll`)
 * Offsets are committed automatically after processing. The timing depends on [CommitStrategy]:
 * - [CommitStrategy.BySize]: Commit every N records (use BySize(1) for per-record)
 * - [CommitStrategy.ByTime]: Commit every interval
 * - [CommitStrategy.BySizeOrTime]: Whichever comes first (balanced)
 *
 * ### Manual-ack (`pollWithAck`)
 * Consumer controls exactly when to commit by calling `acknowledge()`.
 * Commit happens immediately when `acknowledge()` is called.
 * Use this for exactly-once processing with external systems.
 */
interface KafkaPoller<K : Any, V : Any> {
  /**
   * Consumes messages from the specified topic as a Flow with auto-acknowledgment.
   *
   * Offsets are committed automatically based on the configured [CommitStrategy].
   * Use this when you don't need fine-grained commit control.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control (default: [DEFAULT_BUFFER_CAPACITY])
   * @return Flow of consumer records
   */
  fun poll(
    topic: TopicConfig,
    bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY
  ): Flow<ConsumerRecord<K, V>>

  /**
   * Consumes messages with manual acknowledgment support.
   *
   * Each record is wrapped with an acknowledge function that must be called
   * after successful processing to commit the offset.
   *
   * Commit happens immediately when `acknowledge()` is called.
   * Use this when you need exactly-once semantics with external systems.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control (default: [DEFAULT_BUFFER_CAPACITY])
   * @return Flow of ackable records
   */
  fun pollWithAck(
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
