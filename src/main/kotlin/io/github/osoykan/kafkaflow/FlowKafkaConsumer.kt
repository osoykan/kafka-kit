package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.AckableRecord
import io.github.osoykan.kafkaflow.poller.DEFAULT_BUFFER_CAPACITY
import io.github.osoykan.kafkaflow.poller.SpringKafkaPoller
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.support.Acknowledgment
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * A record with its acknowledgment for manual commit.
 *
 * @property record The Kafka consumer record
 * @property acknowledgment The acknowledgment to call after processing
 */
data class AcknowledgingRecord<K, V>(
  val record: ConsumerRecord<K, V>,
  val acknowledgment: Acknowledgment
)

/**
 * Flow-based Kafka consumer using Spring Kafka.
 *
 * Uses Spring Kafka's ConcurrentMessageListenerContainer under the hood,
 * providing a Flow-based API with backpressure handling.
 *
 * ## Features
 * - Virtual Threads support for blocking poll operations (JDK 21+)
 * - Auto/Manual acknowledgment modes
 * - Integrated with Spring Kafka's rebalancing and commit management
 * - Configurable commit strategies via [CommitStrategy]
 * - Backpressure control via configurable buffer capacity
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher (defaults to Dispatchers.IO)
 */
class FlowKafkaConsumer<K : Any, V : Any>(
  consumerFactory: ConsumerFactory<K, V>,
  listenerConfig: ListenerConfig,
  errorHandler: CommonErrorHandler? = null,
  dispatcher: CoroutineDispatcher = Dispatchers.IO
) {
  private val poller = SpringKafkaPoller(consumerFactory, listenerConfig, errorHandler, dispatcher)
  private val stopped = AtomicBoolean(false)

  /**
   * Consumes messages from the specified topic as a Flow.
   *
   * The flow uses backpressure to control message consumption rate.
   * When the buffer is full, the Kafka listener thread blocks until
   * the consumer catches up, preventing unbounded memory growth.
   *
   * Offsets are committed automatically based on [ListenerConfig.commitStrategy].
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control (default: [DEFAULT_BUFFER_CAPACITY])
   * @return Flow of consumer records
   */
  fun consume(
    topic: TopicConfig,
    bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY
  ): Flow<ConsumerRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }
    return poller.poll(topic, bufferCapacity)
  }

  /**
   * Consumes messages with manual acknowledgment support.
   *
   * Each record is wrapped with its acknowledgment object for manual commit control.
   * This is useful when you need to ensure exactly-once processing.
   *
   * The flow uses backpressure to control message consumption rate.
   * When the buffer is full, the Kafka listener thread blocks until
   * the consumer catches up.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control (default: [DEFAULT_BUFFER_CAPACITY])
   * @return Flow of acknowledging records
   */
  fun consumeWithAck(
    topic: TopicConfig,
    bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY
  ): Flow<AcknowledgingRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }
    // Delegate to poller and map to our AcknowledgingRecord type
    return poller.pollWithAck(topic, bufferCapacity).map { ackable ->
      AcknowledgingRecord(
        record = ackable.record,
        acknowledgment = { ackable.acknowledge() }
      )
    }
  }

  /**
   * Stops all active consumers.
   */
  fun stop() {
    if (stopped.compareAndSet(false, true)) {
      poller.stop()
    }
  }

  /**
   * Checks if the consumer is stopped.
   */
  fun isStopped(): Boolean = stopped.get()
}
