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
  private val committers = ConcurrentHashMap<String, CommitterContext>()
  private val stopped = AtomicBoolean(false)

  override fun poll(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> = pollWithOrderedCommits(topic, bufferCapacity)

  override fun stop() {
    if (stopped.compareAndSet(false, true)) {
      logger.info { "SpringKafkaPoller: Stopping all consumers (${containers.size} active)" }
      stopAllContainers()
      flushAndCloseCommitters()
    }
  }

  override fun isStopped(): Boolean = stopped.get()

  /**
   * Polls Kafka with ordered commits using [OrderedCommitter].
   *
   * Backpressure is tracked based on in-flight records:
   * - onBufferAdd() when record enters processing pipeline
   * - onBufferConsume() when record is acknowledged (processing complete)
   *
   * Gap detection pauses consumption when records complete out of order,
   * preventing unbounded memory growth from pending completions.
   */
  private fun pollWithOrderedCommits(
    topic: TopicConfig,
    bufferCapacity: Int
  ): Flow<AckableRecord<K, V>> {
    // Create container reference early so committer can pause/resume on gap detection
    val containerRef = ContainerRef<K, V>()
    val committerContext = createCommitterContext(topic, containerRef)
    committers[topic.displayName] = committerContext

    return callbackFlow {
      if (stopped.get()) {
        close()
        return@callbackFlow
      }

      val committerJob = launch { committerContext.process() }
      val containerContext = createAndStartContainer(topic, bufferCapacity, committerContext.channel, containerRef)

      logStartup(topic, containerContext.container)

      awaitClose {
        shutdownContainer(topic, containerContext, committerJob, committerContext)
      }
    }.buffer(bufferCapacity)
      .flowOn(dispatcher)
  }

  // region Container Management

  private data class ContainerContext<K : Any, V : Any>(
    val container: ConcurrentMessageListenerContainer<K, V>,
    val backpressure: BackpressureController
  )

  private fun ProducerScope<AckableRecord<K, V>>.createAndStartContainer(
    topic: TopicConfig,
    bufferCapacity: Int,
    commitChannel: Channel<CompletionEvent>,
    containerRef: ContainerRef<K, V>
  ): ContainerContext<K, V> {
    val backpressure = BackpressureController(
      containerProvider = { containerRef.get() },
      config = listenerConfig.backpressure,
      bufferCapacity = bufferCapacity,
      topicName = topic.displayName
    )

    val listener = createListener(commitChannel, backpressure)
    val containerProps = createContainerProperties(topic).apply {
      setMessageListener(listener)
    }

    val container = createContainer(topic, containerProps)
    containerRef.set(container)
    containers[topic.displayName] = container
    container.start()

    return ContainerContext(container, backpressure)
  }

  private fun ProducerScope<AckableRecord<K, V>>.createListener(
    commitChannel: Channel<CompletionEvent>,
    backpressure: BackpressureController
  ): AcknowledgingMessageListener<K, V> = AcknowledgingListenerFactory.create(
    commitChannel = commitChannel,
    sendToFlow = { record ->
      trySendBlocking(record).let { channelResult ->
        if (channelResult.isSuccess) {
          Result.success(Unit)
        } else {
          Result.failure(channelResult.exceptionOrNull() ?: Exception("Channel send failed"))
        }
      }
    },
    onRecordEmitted = { backpressure.onBufferAdd() },
    onRecordAcknowledged = { backpressure.onBufferConsume() }
  )

  private fun createContainerProperties(topic: TopicConfig): ContainerProperties =
    ContainerProperties(*topic.topics.toTypedArray()).apply {
      pollTimeout = topic.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
      // Always use MANUAL_IMMEDIATE - OrderedCommitter handles commit ordering
      ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
      idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
      isSyncCommits = true
      syncCommitTimeout = Duration.ofSeconds(5)
      configureVirtualThreads(this, topic)
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

  // endregion

  // region Committer Management

  private data class CommitterContext(
    val committer: OrderedCommitter,
    val channel: Channel<CompletionEvent>
  ) {
    suspend fun process() = committer.processChannel(channel)

    suspend fun flush() {
      channel.close()
      committer.flush()
    }
  }

  private fun createCommitterContext(topic: TopicConfig, containerRef: ContainerRef<K, V>): CommitterContext {
    val effectiveCommitStrategy = topic.effectiveCommitStrategy(listenerConfig.commitStrategy)
    val orderedCommitter = OrderedCommitter(
      commitStrategy = effectiveCommitStrategy,
      onCommit = { partition, offset ->
        logger.debug { "OrderedCommitter[$effectiveCommitStrategy]: Committed partition $partition up to offset $offset" }
      },
      onGapDetected = {
        logger.info { "OrderedCommitter[${topic.displayName}]: Gap detected, pausing container" }
        runCatching { containerRef.get().pause() }
          .onFailure { e -> logger.warn(e) { "Failed to pause container on gap detection" } }
      },
      onGapClosed = {
        logger.info { "OrderedCommitter[${topic.displayName}]: Gap closed, resuming container" }
        runCatching { containerRef.get().resume() }
          .onFailure { e -> logger.warn(e) { "Failed to resume container on gap closure" } }
      }
    )
    return CommitterContext(orderedCommitter, createCommitChannel())
  }

  // endregion

  // region Lifecycle

  private fun stopAllContainers() {
    containers.values.forEach { container ->
      runCatching { container.stop() }
        .onFailure { e -> logger.error(e) { "Error stopping container" } }
    }
    containers.clear()
  }

  private fun flushAndCloseCommitters() {
    runBlocking {
      committers.forEach { (topic, context) ->
        runCatching { context.flush() }
          .onSuccess { logger.info { "SpringKafkaPoller: Flushed ordered committer for [$topic]" } }
          .onFailure { e -> logger.error(e) { "Error flushing committer for [$topic]" } }
      }
    }
    committers.clear()
  }

  private fun shutdownContainer(
    topic: TopicConfig,
    containerContext: ContainerContext<K, V>,
    committerJob: Job,
    committerContext: CommitterContext
  ) {
    logger.info { "SpringKafkaPoller: Stopping consumer for topics: [${topic.displayName}]" }

    containerContext.container.stop()
    containers.remove(topic.displayName)

    committerContext.channel.close()
    runBlocking {
      committerJob.join()
      committerContext.committer.flush()
    }
    committers.remove(topic.displayName)

    logger.info { "SpringKafkaPoller: Ordered committer stopped for [${topic.displayName}]" }
  }

  // endregion

  // region Logging

  private fun logStartup(topic: TopicConfig, container: ConcurrentMessageListenerContainer<K, V>) {
    logger.info {
      "SpringKafkaPoller: Starting consumer for topics: [${topic.displayName}], " +
        "partitions: ${container.concurrency}, " +
        "backpressure: ${if (listenerConfig.backpressure.enabled) "enabled" else "disabled"}, " +
        "orderedCommits: enabled"
    }
  }

  // endregion
}

/**
 * Thread-safe holder for late-initialized container reference.
 * Used to allow gap detection callbacks to pause/resume the container.
 */
internal class ContainerRef<K : Any, V : Any> {
  @Volatile
  private lateinit var container: ConcurrentMessageListenerContainer<K, V>

  fun set(c: ConcurrentMessageListenerContainer<K, V>) {
    container = c
  }

  fun get(): ConcurrentMessageListenerContainer<K, V> = container
}
