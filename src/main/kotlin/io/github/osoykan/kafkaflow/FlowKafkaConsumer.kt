package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.KafkaPoller
import io.github.osoykan.kafkaflow.poller.SpringKafkaPoller
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.*
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
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
 * Flow-based Kafka consumer with pluggable polling backends.
 *
 * Supports two polling implementations:
 * - [PollerType.SPRING_KAFKA]: Uses Spring Kafka's ConcurrentMessageListenerContainer (default)
 * - [PollerType.REACTOR_KAFKA]: Uses Reactor Kafka's KafkaReceiver
 *
 * Both provide the same Flow-based API with backpressure handling.
 *
 * ## Spring Kafka Poller (default)
 * - Virtual Threads support for blocking poll operations
 * - Auto/Manual acknowledgment modes
 * - Integrated with Spring Kafka's rebalancing and commit management
 *
 * ## Reactor Kafka Poller
 * - Non-blocking reactive polling
 * - Virtual Threads via Reactor Schedulers
 * - Partition-based grouping for ordered processing
 *
 * @param poller The underlying Kafka poller implementation
 */
class FlowKafkaConsumer<K : Any, V : Any> private constructor(
  private val poller: KafkaPoller<K, V>,
  private val consumerFactory: ConsumerFactory<K, V>?,
  private val listenerConfig: ListenerConfig?,
  private val errorHandler: CommonErrorHandler?,
  private val dispatcher: CoroutineDispatcher
) {
  private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<K, V>>()
  private val stopped = AtomicBoolean(false)

  /**
   * Creates a FlowKafkaConsumer with Spring Kafka poller (backward compatible).
   *
   * @param consumerFactory The Spring Kafka consumer factory
   * @param listenerConfig Default listener configuration
   * @param errorHandler Optional custom error handler
   * @param dispatcher Coroutine dispatcher (defaults to Dispatchers.IO)
   */
  constructor(
    consumerFactory: ConsumerFactory<K, V>,
    listenerConfig: ListenerConfig,
    errorHandler: CommonErrorHandler? = null,
    dispatcher: CoroutineDispatcher = Dispatchers.IO
  ) : this(
    poller = SpringKafkaPoller(consumerFactory, listenerConfig, errorHandler, dispatcher),
    consumerFactory = consumerFactory,
    listenerConfig = listenerConfig,
    errorHandler = errorHandler,
    dispatcher = dispatcher
  )

  companion object {
    /**
     * Creates a FlowKafkaConsumer with a custom poller implementation.
     *
     * @param poller The Kafka poller to use
     * @return A new FlowKafkaConsumer instance
     */
    fun <K : Any, V : Any> withPoller(poller: KafkaPoller<K, V>): FlowKafkaConsumer<K, V> =
      FlowKafkaConsumer(
        poller = poller,
        consumerFactory = null,
        listenerConfig = null,
        errorHandler = null,
        dispatcher = Dispatchers.IO
      )
  }

  /**
   * Consumes messages from the specified topic as a Flow.
   *
   * The flow uses backpressure to control message consumption rate.
   * Delegates to the configured poller implementation.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure (default: Channel.BUFFERED)
   * @return Flow of consumer records
   */
  fun consume(
    topic: TopicConfig,
    @Suppress("UNUSED_PARAMETER") bufferCapacity: Int = Channel.BUFFERED
  ): Flow<ConsumerRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }
    return poller.poll(topic)
  }

  /**
   * Consumes messages with manual acknowledgment support.
   *
   * Each record is wrapped with its acknowledgment object for manual commit control.
   * This is useful when you need to ensure exactly-once processing.
   *
   * Note: Manual acknowledgment requires Spring Kafka poller. If using Reactor Kafka,
   * configure auto-commit settings in ReceiverOptions instead.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure
   * @return Flow of acknowledging records
   * @throws IllegalStateException if consumerFactory or listenerConfig is not available
   */
  fun consumeWithAck(
    topic: TopicConfig,
    bufferCapacity: Int = Channel.BUFFERED
  ): Flow<AcknowledgingRecord<K, V>> {
    val factory = requireNotNull(consumerFactory) {
      "consumeWithAck requires Spring Kafka poller. Use the constructor with ConsumerFactory."
    }
    val config = requireNotNull(listenerConfig) {
      "consumeWithAck requires Spring Kafka poller. Use the constructor with ListenerConfig."
    }

    return callbackFlow {
      if (stopped.get()) {
        close()
        return@callbackFlow
      }

      val listener = org.springframework.kafka.listener.AcknowledgingMessageListener<K, V> { record, ack ->
        if (ack != null) {
          trySendBlocking(AcknowledgingRecord(record, ack)).exceptionOrNull()?.let { exception ->
            if (exception !is CancellationException) {
              logger.error(exception) { "Failed to emit record with ack from topic ${topic.name}: ${record.key()}" }
            }
          }
        }
      }

      val containerProps = createContainerPropertiesWithAck(topic, config, listener)
      val container = createContainer(topic, factory, containerProps)
      containers[topic.name] = container

      logger.info { "Starting acknowledging consumer for topic: ${topic.name}" }
      container.start()

      awaitClose {
        logger.info { "Stopping acknowledging consumer for topic: ${topic.name}" }
        container.stop()
        containers.remove(topic.name)
      }
    }.buffer(bufferCapacity).flowOn(dispatcher)
  }

  /**
   * Stops all active consumers.
   */
  fun stop() {
    if (stopped.compareAndSet(false, true)) {
      // Stop the poller
      poller.stop()

      // Also stop any manual ack containers
      if (containers.isNotEmpty()) {
        logger.info { "Stopping manual ack containers (${containers.size} active)" }
        containers.values.forEach { container ->
          try {
            container.stop()
          } catch (e: Exception) {
            logger.error(e) { "Error stopping container" }
          }
        }
        containers.clear()
      }
    }
  }

  /**
   * Checks if the consumer is stopped.
   */
  fun isStopped(): Boolean = stopped.get()

  /**
   * Gets the number of active containers.
   */
  fun activeContainerCount(): Int = containers.size

  private fun createContainerPropertiesWithAck(
    topic: TopicConfig,
    config: ListenerConfig,
    listener: AcknowledgingMessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(config.pollTimeout).inWholeMilliseconds
    props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
    props.idleBetweenPolls = config.idleBetweenPolls.inWholeMilliseconds
    // Force sync commits for manual ack to ensure interceptor sees commits
    props.isSyncCommits = true
    props.syncCommitTimeout = Duration.ofMillis(config.syncCommitTimeout.inWholeMilliseconds)
    props.setMessageListener(listener)

    // Use virtual threads for Kafka poll() operations (JDK 21+)
    if (config.useVirtualThreads) {
      val executor = SimpleAsyncTaskExecutor("kafka-flow-ack-").apply {
        setVirtualThreads(true)
      }
      props.listenerTaskExecutor = executor
      logger.debug { "Using virtual threads for polling topic: ${topic.name}" }
    }
  }

  private fun createContainer(
    topic: TopicConfig,
    factory: ConsumerFactory<K, V>,
    containerProps: ContainerProperties
  ): ConcurrentMessageListenerContainer<K, V> = ConcurrentMessageListenerContainer(factory, containerProps).apply {
    concurrency = topic.effectiveConcurrency(listenerConfig?.concurrency ?: 1)
    errorHandler?.let { commonErrorHandler = it }
  }
}
