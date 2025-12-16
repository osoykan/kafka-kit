package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.ListenerConfig
import io.github.osoykan.kafkaflow.TopicConfig
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
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Spring Kafka based poller using ConcurrentMessageListenerContainer.
 *
 * Features:
 * - Virtual Threads support for blocking poll() operations (JDK 21+)
 * - Auto/Manual acknowledgment modes
 * - Configurable concurrency per topic
 *
 * Spring Kafka handles:
 * - Rebalancing
 * - Commit management
 * - Consumer lifecycle
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher for flow emission (default: Dispatchers.IO)
 */
class SpringKafkaPoller<K : Any, V : Any>(
  private val consumerFactory: ConsumerFactory<K, V>,
  private val listenerConfig: ListenerConfig,
  private val errorHandler: CommonErrorHandler? = null,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : KafkaPoller<K, V> {
  private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<K, V>>()
  private val stopped = AtomicBoolean(false)

  /**
   * Consumes messages with auto-acknowledgment based on ListenerConfig.ackMode.
   */
  override fun poll(topic: TopicConfig): Flow<ConsumerRecord<K, V>> = callbackFlow {
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

    val containerProps = createContainerProperties(topic, listener)
    val container = createContainer(topic, containerProps)
    containers["${topic.name}-auto"] = container

    logger.info { "SpringKafkaPoller: Starting auto-ack consumer for topic: ${topic.name} with concurrency: ${container.concurrency}" }
    container.start()

    awaitClose {
      logger.info { "SpringKafkaPoller: Stopping auto-ack consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove("${topic.name}-auto")
    }
  }.buffer(Channel.BUFFERED).flowOn(dispatcher)

  /**
   * Consumes messages with manual acknowledgment.
   *
   * Uses MANUAL_IMMEDIATE ack mode. You must call `acknowledge()` on each record
   * after successful processing to commit the offset.
   *
   * Example:
   * ```kotlin
   * poller.pollWithAck(topic).collect { ackable ->
   *   try {
   *     process(ackable.record)
   *     ackable.acknowledge() // Commit offset
   *   } catch (e: Exception) {
   *     // Don't acknowledge - will be redelivered
   *   }
   * }
   * ```
   */
  override fun pollWithAck(topic: TopicConfig): Flow<AckableRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = org.springframework.kafka.listener.AcknowledgingMessageListener<K, V> { record, ack ->
      if (ack != null) {
        val ackableRecord = AckableRecord(
          record = record,
          acknowledge = { ack.acknowledge() }
        )
        trySendBlocking(ackableRecord).exceptionOrNull()?.let { exception ->
          if (exception !is CancellationException) {
            logger.error(exception) { "Failed to emit record with ack from topic ${topic.name}: ${record.key()}" }
          }
        }
      }
    }

    val containerProps = createContainerPropertiesWithAck(topic, listener)
    val container = createContainer(topic, containerProps)
    containers["${topic.name}-manual"] = container

    logger.info { "SpringKafkaPoller: Starting manual-ack consumer for topic: ${topic.name} with concurrency: ${container.concurrency}" }
    container.start()

    awaitClose {
      logger.info { "SpringKafkaPoller: Stopping manual-ack consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove("${topic.name}-manual")
    }
  }.buffer(Channel.BUFFERED).flowOn(dispatcher)

  override fun stop() {
    if (stopped.compareAndSet(false, true)) {
      logger.info { "SpringKafkaPoller: Stopping all consumers (${containers.size} active)" }
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

  override fun isStopped(): Boolean = stopped.get()

  private fun createContainerProperties(
    topic: TopicConfig,
    listener: MessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
    // For auto-ack (MessageListener), use RECORD mode to commit after each message
    // This ensures the consumer interceptor sees committed messages
    props.ackMode = ContainerProperties.AckMode.RECORD
    props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
    props.isSyncCommits = listenerConfig.syncCommits
    props.syncCommitTimeout = Duration.ofMillis(listenerConfig.syncCommitTimeout.inWholeMilliseconds)
    props.setMessageListener(listener)
    configureVirtualThreads(props, topic)
  }

  private fun createContainerPropertiesWithAck(
    topic: TopicConfig,
    listener: org.springframework.kafka.listener.AcknowledgingMessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
    props.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
    props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
    props.isSyncCommits = listenerConfig.syncCommits
    props.syncCommitTimeout = Duration.ofMillis(listenerConfig.syncCommitTimeout.inWholeMilliseconds)
    props.setMessageListener(listener)
    configureVirtualThreads(props, topic)
  }

  private fun configureVirtualThreads(props: ContainerProperties, topic: TopicConfig) {
    if (listenerConfig.useVirtualThreads) {
      val executor = SimpleAsyncTaskExecutor("spring-kafka-vt-").apply {
        setVirtualThreads(true)
      }
      props.listenerTaskExecutor = executor
      logger.debug { "SpringKafkaPoller: Using virtual threads for topic: ${topic.name}" }
    }
  }

  private fun createContainer(
    topic: TopicConfig,
    containerProps: ContainerProperties
  ): ConcurrentMessageListenerContainer<K, V> = ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
    concurrency = topic.effectiveConcurrency(listenerConfig.concurrency)
    errorHandler?.let { commonErrorHandler = it }
  }
}
