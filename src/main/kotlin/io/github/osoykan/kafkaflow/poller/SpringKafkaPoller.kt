package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.*
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Acknowledgment mode for the poller.
 */
enum class AckMode {
  /**
   * Auto-ack: Spring Kafka commits based on [CommitStrategy].
   * The [AckableRecord.acknowledge] is a no-op.
   */
  AUTO,

  /**
   * Manual-ack: User calls [AckableRecord.acknowledge] to commit.
   * Uses MANUAL_IMMEDIATE - commit happens immediately.
   */
  MANUAL
}

/**
 * Spring Kafka based poller using ConcurrentMessageListenerContainer.
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param ackMode Acknowledgment mode (AUTO or MANUAL)
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher for flow emission
 */
class SpringKafkaPoller<K : Any, V : Any>(
  private val consumerFactory: ConsumerFactory<K, V>,
  private val listenerConfig: ListenerConfig,
  private val ackMode: AckMode = AckMode.AUTO,
  private val errorHandler: CommonErrorHandler? = null,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : KafkaPoller<K, V> {
  private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<K, V>>()
  private val stopped = AtomicBoolean(false)

  override fun poll(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = when (ackMode) {
    AckMode.AUTO -> pollAutoAck(topic, bufferCapacity)
    AckMode.MANUAL -> pollManualAck(topic, bufferCapacity)
  }

  /**
   * Auto-ack: Spring Kafka commits based on CommitStrategy.
   */
  private fun pollAutoAck(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = MessageListener<K, V> { record ->
      val ackableRecord = AckableRecord(
        record = record,
        acknowledge = { /* no-op - Spring Kafka auto-commits */ }
      )
      trySendBlocking(ackableRecord).exceptionOrNull()?.let { exception ->
        if (exception !is CancellationException) {
          logger.error(exception) { "Failed to emit record from topic ${topic.name}: ${record.key()}" }
        }
      }
    }

    val containerProps = createAutoAckContainerProperties(topic, listener)
    val container = createContainer(topic, containerProps)
    containers["${topic.name}-auto"] = container

    logger.info {
      "SpringKafkaPoller: Starting auto-ack consumer for topic: ${topic.name}, " +
        "partitions: ${container.concurrency}, strategy: ${listenerConfig.commitStrategy}"
    }
    container.start()

    awaitClose {
      logger.info { "SpringKafkaPoller: Stopping auto-ack consumer for topic: ${topic.name}" }
      container.stop()
      containers.remove("${topic.name}-auto")
    }
  }.buffer(bufferCapacity).flowOn(dispatcher)

  /**
   * Manual-ack: User controls when to commit via acknowledge().
   */
  private fun pollManualAck(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = callbackFlow {
    if (stopped.get()) {
      close()
      return@callbackFlow
    }

    val listener = AcknowledgingMessageListener<K, V> { record, ack ->
      if (ack != null) {
        val ackableRecord = AckableRecord(
          record = record,
          acknowledge = { ack.acknowledge() }
        )
        trySendBlocking(ackableRecord).exceptionOrNull()?.let { exception ->
          if (exception !is CancellationException) {
            logger.error(exception) { "Failed to emit record from topic ${topic.name}: ${record.key()}" }
          }
        }
      }
    }

    val containerProps = createManualAckContainerProperties(topic, listener)
    val container = createContainer(topic, containerProps)
    containers["${topic.name}-manual"] = container

    logger.info { "SpringKafkaPoller: Starting manual-ack consumer for topic: ${topic.name}, partitions: ${container.concurrency}" }
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

  private fun createAutoAckContainerProperties(
    topic: TopicConfig,
    listener: MessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds

    // Map CommitStrategy to Spring Kafka AckMode
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
    props.isSyncCommits = listenerConfig.commitStrategy.syncCommits
    props.syncCommitTimeout = Duration.ofMillis(listenerConfig.commitStrategy.syncCommitTimeout.inWholeMilliseconds)
    props.setMessageListener(listener)
    configureVirtualThreads(props, topic)
  }

  private fun createManualAckContainerProperties(
    topic: TopicConfig,
    listener: AcknowledgingMessageListener<K, V>
  ): ContainerProperties = ContainerProperties(topic.name).also { props ->
    props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
    props.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
    props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
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
  ): ConcurrentMessageListenerContainer<K, V> =
    ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
      // Container concurrency controls how many Kafka consumer threads/partitions are used
      concurrency = topic.effectiveMultiplePartitions(listenerConfig.multiplePartitions)
      errorHandler?.let { commonErrorHandler = it }
    }
}
