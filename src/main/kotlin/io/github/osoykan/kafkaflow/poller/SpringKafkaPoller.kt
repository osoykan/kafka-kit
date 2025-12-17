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
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * Controls backpressure by pausing/resuming the Kafka container based on buffer fill level.
 */
private class BackpressureController(
  private val containerProvider: () -> ConcurrentMessageListenerContainer<*, *>,
  private val config: BackpressureConfig,
  private val bufferCapacity: Int,
  private val topicName: String
) {
  private val bufferCount = AtomicInteger(0)
  private val paused = AtomicBoolean(false)

  private val pauseThresholdCount = (bufferCapacity * config.pauseThreshold).toInt()
  private val resumeThresholdCount = (bufferCapacity * config.resumeThreshold).toInt()

  private val container: ConcurrentMessageListenerContainer<*, *>
    get() = containerProvider()

  /**
   * Called when a record is added to the buffer. May pause the container.
   */
  fun onBufferAdd() {
    if (!config.enabled) return

    val count = bufferCount.incrementAndGet()
    if (count >= pauseThresholdCount && paused.compareAndSet(false, true)) {
      container.pause()
      logger.info { "Backpressure: Paused container for $topicName (buffer: $count/$bufferCapacity)" }
    }
  }

  /**
   * Called when a record is consumed from the buffer. May resume the container.
   */
  fun onBufferConsume() {
    if (!config.enabled) return

    val count = bufferCount.decrementAndGet()
    if (count <= resumeThresholdCount && paused.compareAndSet(true, false)) {
      container.resume()
      logger.info { "Backpressure: Resumed container for $topicName (buffer: $count/$bufferCapacity)" }
    }
  }

  fun currentBufferSize(): Int = bufferCount.get()

  fun isPaused(): Boolean = paused.get()
}

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
  ): Flow<AckableRecord<K, V>> {
    lateinit var backpressure: BackpressureController

    return callbackFlow {
      if (stopped.get()) {
        close()
        return@callbackFlow
      }

      // Create backpressure controller with lazy container reference
      val containerRef = object {
        lateinit var container: ConcurrentMessageListenerContainer<K, V>
      }
      backpressure = BackpressureController(
        containerProvider = { containerRef.container },
        config = listenerConfig.backpressure,
        bufferCapacity = bufferCapacity,
        topicName = topic.displayName
      )

      // Create the listener with backpressure support
      val listener = MessageListener<K, V> { record ->
        val ackableRecord = AckableRecord(
          record = record,
          acknowledge = { /* no-op - Spring Kafka auto-commits */ }
        )
        trySendBlocking(ackableRecord).exceptionOrNull()?.let { exception ->
          if (exception !is CancellationException) {
            logger.error(exception) { "Failed to emit record from topic ${record.topic()}: ${record.key()}" }
          }
        }
        backpressure.onBufferAdd()
      }

      // Create container with listener set
      val containerProps = createAutoAckContainerProperties(topic)
      containerProps.setMessageListener(listener)
      val container = createContainer(topic, containerProps)
      containerRef.container = container
      containers["${topic.displayName}-auto"] = container

      logger.info {
        "SpringKafkaPoller: Starting auto-ack consumer for topics: [${topic.displayName}], " +
          "partitions: ${container.concurrency}, strategy: ${listenerConfig.commitStrategy}, " +
          "backpressure: ${if (listenerConfig.backpressure.enabled) "enabled" else "disabled"}"
      }
      container.start()

      awaitClose {
        logger.info { "SpringKafkaPoller: Stopping auto-ack consumer for topics: [${topic.displayName}]" }
        container.stop()
        containers.remove("${topic.displayName}-auto")
      }
    }.buffer(bufferCapacity)
      .onEach { backpressure.onBufferConsume() }
      .flowOn(dispatcher)
  }

  /**
   * Manual-ack: User controls when to commit via acknowledge().
   */
  private fun pollManualAck(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> {
    lateinit var backpressure: BackpressureController

    return callbackFlow {
      if (stopped.get()) {
        close()
        return@callbackFlow
      }

      // Create backpressure controller with lazy container reference
      val containerRef = object {
        lateinit var container: ConcurrentMessageListenerContainer<K, V>
      }
      backpressure = BackpressureController(
        containerProvider = { containerRef.container },
        config = listenerConfig.backpressure,
        bufferCapacity = bufferCapacity,
        topicName = topic.displayName
      )

      // Create the listener with backpressure support
      val listener = AcknowledgingMessageListener<K, V> { record, ack ->
        if (ack != null) {
          val ackableRecord = AckableRecord(
            record = record,
            acknowledge = { ack.acknowledge() }
          )
          trySendBlocking(ackableRecord).exceptionOrNull()?.let { exception ->
            if (exception !is CancellationException) {
              logger.error(exception) { "Failed to emit record from topic ${record.topic()}: ${record.key()}" }
            }
          }
          backpressure.onBufferAdd()
        }
      }

      // Create container with listener set
      val containerProps = createManualAckContainerProperties(topic)
      containerProps.setMessageListener(listener)
      val container = createContainer(topic, containerProps)
      containerRef.container = container
      containers["${topic.displayName}-manual"] = container

      logger.info {
        "SpringKafkaPoller: Starting manual-ack consumer for topics: [${topic.displayName}], " +
          "partitions: ${container.concurrency}, " +
          "backpressure: ${if (listenerConfig.backpressure.enabled) "enabled" else "disabled"}"
      }
      container.start()

      awaitClose {
        logger.info { "SpringKafkaPoller: Stopping manual-ack consumer for topics: [${topic.displayName}]" }
        container.stop()
        containers.remove("${topic.displayName}-manual")
      }
    }.buffer(bufferCapacity)
      .onEach { backpressure.onBufferConsume() }
      .flowOn(dispatcher)
  }

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

  private fun createAutoAckContainerProperties(topic: TopicConfig): ContainerProperties =
    ContainerProperties(*topic.topics.toTypedArray()).also { props ->
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
      configureVirtualThreads(props, topic)
    }

  private fun createManualAckContainerProperties(topic: TopicConfig): ContainerProperties =
    ContainerProperties(*topic.topics.toTypedArray()).also { props ->
      props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
      props.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
      props.idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
      props.isSyncCommits = true
      props.syncCommitTimeout = Duration.ofSeconds(5)
      configureVirtualThreads(props, topic)
    }

  private fun configureVirtualThreads(props: ContainerProperties, topic: TopicConfig) {
    val executor = SimpleAsyncTaskExecutor("spring-kafka-vt-").apply { setVirtualThreads(true) }
    props.listenerTaskExecutor = executor
    logger.debug { "SpringKafkaPoller: Using virtual threads for topics: [${topic.displayName}]" }
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
