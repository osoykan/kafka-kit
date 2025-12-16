package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.*
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
   * Consumes messages with auto-acknowledgment based on ListenerConfig.commitStrategy.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Controls backpressure - when buffer is full, Kafka listener blocks
   */
  override fun poll(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<ConsumerRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = MessageListener<K, V> { record ->
      // trySendBlocking will block when buffer is full, providing backpressure
      trySendBlocking(record).exceptionOrNull()?.let { exception ->
        if (exception !is CancellationException) {
          logger.error(exception) { "Failed to emit record from topic ${topic.name}: ${record.key()}" }
        }
      }
    }

    val containerProps = createContainerProperties(topic, listener)
    val container = createContainer(topic, containerProps)
    containers["${topic.name}-auto"] = container

    logger.info {
      "SpringKafkaPoller: Starting auto-ack consumer for topic: ${topic.name} " +
        "concurrency: ${container.concurrency}, bufferCapacity: $bufferCapacity"
    }
    container.start()

    awaitClose {
      logger.info { "SpringKafkaPoller: Stopping auto-ack consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove("${topic.name}-auto")
    }
  }.buffer(bufferCapacity).flowOn(dispatcher)

  /**
   * Consumes messages with manual acknowledgment.
   *
   * Uses MANUAL_IMMEDIATE ack mode. You must call `acknowledge()` on each record
   * after successful processing to commit the offset.
   *
   * @param topic Topic configuration
   * @param bufferCapacity Controls backpressure - when buffer is full, Kafka listener blocks
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
  override fun pollWithAck(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = AcknowledgingMessageListener { record, ack ->
      if (ack != null) {
        val ackableRecord = AckableRecord(
          record = record,
          acknowledge = { ack.acknowledge() }
        )
        // trySendBlocking will block when buffer is full, providing backpressure
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

    logger.info {
      "SpringKafkaPoller: Starting manual-ack consumer for topic: ${topic.name} " +
        "concurrency: ${container.concurrency}, bufferCapacity: $bufferCapacity"
    }
    container.start()

    awaitClose {
      logger.info { "SpringKafkaPoller: Stopping manual-ack consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove("${topic.name}-manual")
    }
  }.buffer(bufferCapacity).flowOn(dispatcher)

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

    // Map CommitStrategy to Spring Kafka AckMode for auto-ack consumers
    when (val strategy = listenerConfig.commitStrategy) {
      is CommitStrategy.BySize -> {
        if (strategy.size == 1) {
          props.ackMode = ContainerProperties.AckMode.RECORD
        } else {
          props.ackMode = ContainerProperties.AckMode.COUNT
          props.ackCount = strategy.size
        }
      }

      is CommitStrategy.ByTime -> {
        props.ackMode = ContainerProperties.AckMode.TIME
        props.setAckTime(strategy.interval.inWholeMilliseconds)
      }

      is CommitStrategy.BySizeOrTime -> {
        props.ackMode = ContainerProperties.AckMode.COUNT_TIME
        props.ackCount = strategy.size
        props.setAckTime(strategy.interval.inWholeMilliseconds)
      }
    }

    props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
    // Use sync commits for safety - ensures offsets are committed before continuing
    props.isSyncCommits = true
    props.syncCommitTimeout = Duration.ofSeconds(5)
    props.setMessageListener(listener)
    configureVirtualThreads(props, topic)
  }

  private fun createContainerPropertiesWithAck(
    topic: TopicConfig,
    listener: AcknowledgingMessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
    props.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
    props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
    // Use sync commits for manual ack - ensures exactly-once semantics
    props.isSyncCommits = true
    props.syncCommitTimeout = Duration.ofSeconds(5)
    props.setMessageListener(listener)
    configureVirtualThreads(props, topic)
  }

  private fun configureVirtualThreads(props: ContainerProperties, topic: TopicConfig) {
    val executor = SimpleAsyncTaskExecutor("spring-kafka-vt-").apply { setVirtualThreads(true) }
    props.listenerTaskExecutor = executor
    logger.debug { "SpringKafkaPoller: Using virtual threads for topic: ${topic.name}" }
  }

  private fun createContainer(
    topic: TopicConfig,
    containerProps: ContainerProperties
  ): ConcurrentMessageListenerContainer<K, V> = ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
    concurrency = topic.effectiveConcurrency(listenerConfig.concurrency)
    errorHandler?.let { commonErrorHandler = it }
  }
}
