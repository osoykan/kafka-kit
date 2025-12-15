package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flowOn
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.MessageListener
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
 * Flow-based Kafka consumer that wraps Spring Kafka's MessageListenerContainer.
 *
 * Provides Kotlin Flow integration with proper backpressure handling and
 * coroutine support using Dispatchers.IO for non-blocking operation.
 *
 * Spring Kafka handles all the complexity of:
 * - Rebalancing
 * - Commit management
 * - Consumer lifecycle
 *
 * This wrapper focuses on providing a clean Flow-based API for consuming records.
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher (defaults to Dispatchers.IO)
 */
class FlowKafkaConsumer<K : Any, V : Any>(
  private val consumerFactory: ConsumerFactory<K, V>,
  private val listenerConfig: ListenerConfig,
  private val errorHandler: CommonErrorHandler? = null,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) {
  private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<K, V>>()
  private val stopped = AtomicBoolean(false)

  /**
   * Consumes messages from the specified topic as a Flow.
   *
   * The flow uses backpressure to control message consumption rate.
   * Messages are emitted on the configured dispatcher (default: Dispatchers.IO).
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure (default: Channel.BUFFERED)
   * @return Flow of consumer records
   */
  fun consume(
    topic: TopicConfig,
    bufferCapacity: Int = Channel.BUFFERED
  ): Flow<ConsumerRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = MessageListener<K, V> { record ->
      trySendBlocking(record).exceptionOrNull()?.let { exception ->
        if (exception !is CancellationException) {
          logger.error(exception) { "Failed to emit record from topic ${topic.name}: ${record.key()}" }
        }
      }
    }

    val containerProps = createContainerProperties(topic, listenerConfig, listener)
    val container = createContainer(topic, containerProps)
    containers[topic.name] = container

    logger.info { "Starting consumer for topic: ${topic.name} with concurrency: ${container.concurrency}" }
    container.start()

    awaitClose {
      logger.info { "Stopping consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove(topic.name)
    }
  }.buffer(bufferCapacity).flowOn(dispatcher)

  /**
   * Consumes messages with manual acknowledgment support.
   *
   * Each record is wrapped with its acknowledgment object for manual commit control.
   * This is useful when you need to ensure exactly-once processing.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Buffer capacity for backpressure
   * @return Flow of acknowledging records
   */
  fun consumeWithAck(
    topic: TopicConfig,
    bufferCapacity: Int = Channel.BUFFERED
  ): Flow<AcknowledgingRecord<K, V>> = callbackFlow {
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

    val containerProps = createContainerPropertiesWithAck(topic, listenerConfig, listener)
    val container = createContainer(topic, containerProps)
    containers[topic.name] = container

    logger.info { "Starting acknowledging consumer for topic: ${topic.name}" }
    container.start()

    awaitClose {
      logger.info { "Stopping acknowledging consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove(topic.name)
    }
  }.buffer(bufferCapacity).flowOn(dispatcher)

  /**
   * Stops all active consumers.
   */
  fun stop() {
    if (stopped.compareAndSet(false, true)) {
      logger.info { "Stopping all consumers (${containers.size} active)" }
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

  /**
   * Checks if the consumer is stopped.
   */
  fun isStopped(): Boolean = stopped.get()

  /**
   * Gets the number of active containers.
   */
  fun activeContainerCount(): Int = containers.size

  private fun createContainerProperties(
    topic: TopicConfig,
    config: ListenerConfig,
    listener: MessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.setPollTimeout(topic.effectivePollTimeout(config.pollTimeout).inWholeMilliseconds)
    props.setAckMode(config.ackMode.toSpringAckMode())
    props.setIdleBetweenPolls(config.idleBetweenPolls.inWholeMilliseconds)
    props.isSyncCommits = config.syncCommits
    props.setSyncCommitTimeout(Duration.ofMillis(config.syncCommitTimeout.inWholeMilliseconds))
    props.setMessageListener(listener)

    // Use virtual threads for Kafka poll() operations (JDK 21+)
    // This is ideal for blocking I/O like consumer.poll()
    if (config.useVirtualThreads) {
      val executor = SimpleAsyncTaskExecutor("kafka-flow-").apply {
        setVirtualThreads(true)
      }
      props.listenerTaskExecutor = executor
      logger.debug { "Using virtual threads for polling topic: ${topic.name}" }
    }
  }

  private fun createContainerPropertiesWithAck(
    topic: TopicConfig,
    config: ListenerConfig,
    listener: org.springframework.kafka.listener.AcknowledgingMessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.setPollTimeout(topic.effectivePollTimeout(config.pollTimeout).inWholeMilliseconds)
    props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
    props.setIdleBetweenPolls(config.idleBetweenPolls.inWholeMilliseconds)
    props.isSyncCommits = config.syncCommits
    props.setSyncCommitTimeout(Duration.ofMillis(config.syncCommitTimeout.inWholeMilliseconds))
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
    containerProps: ContainerProperties
  ): ConcurrentMessageListenerContainer<K, V> = ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
    concurrency = topic.effectiveConcurrency(listenerConfig.concurrency)
    errorHandler?.let { setCommonErrorHandler(it) }
  }
}
