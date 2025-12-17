package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.AckMode
import io.github.osoykan.kafkaflow.poller.AckableRecord
import io.github.osoykan.kafkaflow.poller.DEFAULT_BUFFER_CAPACITY
import io.github.osoykan.kafkaflow.poller.SpringKafkaPoller
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Flow-based Kafka consumer using Spring Kafka.
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param ackMode Acknowledgment mode (AUTO or MANUAL)
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher (defaults to Dispatchers.IO)
 */
class FlowKafkaConsumer<K : Any, V : Any>(
  consumerFactory: ConsumerFactory<K, V>,
  listenerConfig: ListenerConfig,
  ackMode: AckMode = AckMode.AUTO,
  errorHandler: CommonErrorHandler? = null,
  dispatcher: CoroutineDispatcher = Dispatchers.IO
) {
  private val poller = SpringKafkaPoller(consumerFactory, listenerConfig, ackMode, errorHandler, dispatcher)
  private val stopped = AtomicBoolean(false)

  /**
   * Consumes messages from the specified topic.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure control
   * @return Flow of ackable records
   */
  fun consume(
    topic: TopicConfig,
    bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY
  ): Flow<AckableRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }
    return poller.poll(topic, bufferCapacity)
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
