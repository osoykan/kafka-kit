package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.*
import org.springframework.kafka.support.Acknowledgment
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

  fun onBufferAdd() {
    if (!config.enabled) return

    val count = bufferCount.incrementAndGet()
    if (count >= pauseThresholdCount && paused.compareAndSet(false, true)) {
      container.pause()
      logger.info { "Backpressure: Paused container for $topicName (buffer: $count/$bufferCapacity)" }
    }
  }

  fun onBufferConsume() {
    if (!config.enabled) return

    val count = bufferCount.decrementAndGet()
    if (count <= resumeThresholdCount && paused.compareAndSet(true, false)) {
      container.resume()
      logger.info { "Backpressure: Resumed container for $topicName (buffer: $count/$bufferCapacity)" }
    }
  }
}

/**
 * Spring Kafka based poller using ConcurrentMessageListenerContainer.
 *
 * Uses [OrderedCommitter] to ensure offsets are committed in order even when
 * records are processed concurrently (e.g., with flatMapMerge). This prevents
 * offset gaps that could cause message loss on restart.
 *
 * ## How it works
 *
 * 1. Records are consumed from Kafka in order (per partition)
 * 2. Each record is wrapped in [AckableRecord] with an `acknowledge()` callback
 * 3. When user calls `acknowledge()`, it sends a [CompletionEvent] to the committer
 * 4. The [OrderedCommitter] tracks completed offsets per partition
 * 5. Only when offsets are contiguous does the actual Kafka commit happen
 *
 * ## Example
 *
 * ```
 * Records consumed:    0, 1, 2, 3, 4
 * Processing order:    4, 2, 0, 3, 1 (due to concurrent processing)
 * User ack order:      4, 2, 0, 3, 1 (same as processing)
 * Actual commit order: 0, 1, 2, 3, 4 (OrderedCommitter ensures this)
 * ```
 *
 * @param consumerFactory The Spring Kafka consumer factory
 * @param listenerConfig Default listener configuration
 * @param errorHandler Optional custom error handler
 * @param dispatcher Coroutine dispatcher for flow emission
 */
class SpringKafkaPoller<K : Any, V : Any>(
  private val consumerFactory: ConsumerFactory<K, V>,
  private val listenerConfig: ListenerConfig,
  private val errorHandler: CommonErrorHandler? = null,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : KafkaPoller<K, V> {
  private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<K, V>>()
  private val committers = ConcurrentHashMap<String, Pair<OrderedCommitter, Channel<CompletionEvent>>>()
  private val stopped = AtomicBoolean(false)

  override fun poll(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = pollWithOrderedCommits(topic, bufferCapacity)

  /**
   * Polls Kafka with ordered commits using [OrderedCommitter].
   * User's acknowledge() calls go through the committer to ensure
   * offsets are committed in order.
   */
  private fun pollWithOrderedCommits(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> {
    lateinit var backpressure: BackpressureController

    // Create ordered committer for this topic with configured commit strategy
    val orderedCommitter = OrderedCommitter(
      commitStrategy = listenerConfig.commitStrategy,
      onCommit = { partition, offset ->
        logger.debug { "OrderedCommitter: Committed partition $partition up to offset $offset" }
      }
    )
    val commitChannel = createCommitChannel()
    committers[topic.displayName] = orderedCommitter to commitChannel

    return callbackFlow {
      if (stopped.get()) {
        close()
        return@callbackFlow
      }

      // Launch committer job in background
      val committerJob = launch {
        orderedCommitter.processChannel(commitChannel)
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

      // Create acknowledging listener - always use MANUAL mode internally
      // so we can control commit order via OrderedCommitter
      val listener = AcknowledgingMessageListener<K, V> { record, ack ->
        if (ack != null) {
          val ackableRecord = AckableRecord(
            record = record,
            acknowledge = {
              // Send to ordered committer instead of direct ack
              val event = CompletionEvent(
                partition = record.partition(),
                offset = record.offset(),
                acknowledge = { ack.acknowledge() }
              )
              val result = commitChannel.trySend(event)
              if (result.isFailure) {
                logger.warn {
                  "Failed to send completion event for ${record.topic()}:${record.partition()}:${record.offset()}"
                }
                // Fallback to direct ack if channel fails (shouldn't happen with UNLIMITED)
                ack.acknowledge()
              }
            }
          )
          trySendBlocking(ackableRecord)
            .onSuccess {
              backpressure.onBufferAdd()
            }.onFailure { exception ->
              if (exception !is CancellationException) {
                logger.error(exception) { "Failed to emit record from topic ${record.topic()}: ${record.key()}" }
              }
            }
        }
      }

      // Create container with MANUAL_IMMEDIATE mode
      val containerProps = createContainerProperties(topic)
      containerProps.setMessageListener(listener)
      val container = createContainer(topic, containerProps)
      containerRef.container = container
      containers[topic.displayName] = container

      logger.info {
        "SpringKafkaPoller: Starting consumer for topics: [${topic.displayName}], " +
          "partitions: ${container.concurrency}, " +
          "backpressure: ${if (listenerConfig.backpressure.enabled) "enabled" else "disabled"}, " +
          "orderedCommits: enabled"
      }
      container.start()

      awaitClose {
        logger.info { "SpringKafkaPoller: Stopping consumer for topics: [${topic.displayName}]" }
        container.stop()
        containers.remove(topic.displayName)

        // Shutdown committer
        commitChannel.close()
        runBlocking { committerJob.join() }
        orderedCommitter.flush() // Ensure all pending are committed
        committers.remove(topic.displayName)
        logger.info { "SpringKafkaPoller: Ordered committer stopped for [${topic.displayName}]" }
      }
    }.buffer(bufferCapacity)
      .onEach { backpressure.onBufferConsume() }
      .flowOn(dispatcher)
  }

  override fun stop() {
    if (stopped.compareAndSet(false, true)) {
      logger.info { "SpringKafkaPoller: Stopping all consumers (${containers.size} active)" }

      // Stop containers first
      containers.values.forEach { container ->
        try {
          container.stop()
        } catch (e: Exception) {
          logger.error(e) { "Error stopping container" }
        }
      }
      containers.clear()

      // Stop and flush all ordered committers
      committers.forEach { (topic, pair) ->
        val (committer, channel) = pair
        try {
          channel.close()
          committer.flush()
          logger.info { "SpringKafkaPoller: Flushed ordered committer for [$topic]" }
        } catch (e: Exception) {
          logger.error(e) { "Error flushing committer for [$topic]" }
        }
      }
      committers.clear()
    }
  }

  override fun isStopped(): Boolean = stopped.get()

  private fun createContainerProperties(topic: TopicConfig): ContainerProperties =
    ContainerProperties(*topic.topics.toTypedArray()).also { props ->
      props.pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
      // Always use MANUAL_IMMEDIATE - OrderedCommitter handles commit ordering
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
      concurrency = topic.effectiveMultiplePartitions(listenerConfig.multiplePartitions)
      errorHandler?.let { commonErrorHandler = it }
    }
}
