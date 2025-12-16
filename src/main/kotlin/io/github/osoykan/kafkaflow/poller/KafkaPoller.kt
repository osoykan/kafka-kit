package io.github.osoykan.kafkaflow.poller

import io.github.osoykan.kafkaflow.TopicConfig
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
 * Abstraction for Kafka polling mechanisms.
 *
 * Implementations can use different underlying libraries:
 * - [SpringKafkaPoller]: Uses Spring Kafka's ConcurrentMessageListenerContainer
 * - [ReactorKafkaPoller]: Uses Reactor Kafka's KafkaReceiver
 *
 * Both provide the same Flow-based API for consuming records.
 */
interface KafkaPoller<K : Any, V : Any> {
  /**
   * Consumes messages from the specified topic as a Flow with auto-acknowledgment.
   *
   * The flow uses backpressure to control message consumption rate.
   * Offsets are committed automatically based on the poller's configuration.
   *
   * @param topic Topic configuration
   * @return Flow of consumer records
   */
  fun poll(topic: TopicConfig): Flow<ConsumerRecord<K, V>>

  /**
   * Consumes messages with manual acknowledgment support.
   *
   * Each record is wrapped with an acknowledge function that must be called
   * after successful processing to commit the offset.
   *
   * @param topic Topic configuration
   * @return Flow of ackable records
   */
  fun pollWithAck(topic: TopicConfig): Flow<AckableRecord<K, V>>

  /**
   * Stops all active polling.
   */
  fun stop()

  /**
   * Checks if the poller is stopped.
   */
  fun isStopped(): Boolean
}

/**
 * Type of Kafka poller to use.
 */
enum class PollerType {
  /**
   * Spring Kafka based poller using ConcurrentMessageListenerContainer.
   * Supports Virtual Threads for blocking poll operations.
   */
  SPRING_KAFKA,

  /**
   * Reactor Kafka based poller using KafkaReceiver.
   * Uses Project Reactor's scheduler (boundedElastic by default).
   */
  REACTOR_KAFKA
}
