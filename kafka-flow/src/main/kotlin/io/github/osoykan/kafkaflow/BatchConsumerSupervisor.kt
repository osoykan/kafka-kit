package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.nanoseconds

/**
 * Abstract base supervisor for batch consumers.
 *
 * Manages [ConcurrentMessageListenerContainer] lifecycle with [BatchAcknowledgingMessageListener].
 * No flow pipeline, no [OrderedCommitter] - the entire batch is acknowledged after processing.
 * No automatic retry - the user handles per-record failures via [FailureHandler].
 *
 * Subclasses only differ in how they invoke the consumer and handle acknowledgment.
 */
abstract class AbstractBatchConsumerSupervisor<K : Any, V : Any>(
  private val consumer: Consumer<K, V>,
  private val config: ResolvedConsumerConfig,
  private val consumerFactory: ConsumerFactory<K, V>,
  protected val kafkaTemplate: KafkaTemplate<K, V>,
  private val listenerConfig: ListenerConfig,
  private val metrics: KafkaFlowMetrics = NoOpMetrics
) : ConsumerSupervisor {
  protected val log = KotlinLogging.logger("BatchSupervisor[${consumer.consumerName}]")
  protected val failureHandler = DefaultFailureHandler<K, V>(kafkaTemplate, config)

  private val containers = mutableListOf<ConcurrentMessageListenerContainer<K, V>>()
  private val running = AtomicBoolean(false)

  override val consumerName: String = consumer.consumerName
  override val topics: List<String> = config.topic.topics + config.retryTopic

  override fun start() {
    if (running.getAndSet(true)) {
      log.warn { "Consumer already running" }
      return
    }
    log.info { "Starting batch consumer for topics: [${config.topic.displayName}]" }
    startContainer(config.topic)
    startContainer(TopicConfig(name = config.retryTopic))
  }

  /**
   * Invoked inside the batch listener. Subclasses bridge to the consumer's
   * suspend function and handle acknowledgment.
   */
  protected abstract fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  )

  private fun startContainer(topicConfig: TopicConfig) {
    val containerProps = createContainerProperties(topicConfig)
    containerProps.setMessageListener(
      BatchAcknowledgingMessageListener<K, V> { records, ack ->
        val startTime = System.nanoTime()
        try {
          metrics.recordBatchConsumed(topicConfig.displayName, consumerName, records.size)
          onBatch(records, failureHandler, ack)
          val duration = (System.nanoTime() - startTime).nanoseconds
          metrics.recordBatchProcessingSuccess(topicConfig.displayName, consumerName, records.size, duration)
        } catch (e: Exception) {
          log.error(e) { "Batch processing failed for topic: ${topicConfig.displayName}" }
          metrics.recordBatchProcessingFailure(topicConfig.displayName, consumerName, records.size, e)
          throw e
        }
      }
    )

    val container = ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
      concurrency = topicConfig.effectiveMultiplePartitions(listenerConfig.multiplePartitions)
    }
    containers.add(container)
    container.start()
    log.info { "Started batch container for [${topicConfig.displayName}]" }
  }

  private fun createContainerProperties(topicConfig: TopicConfig): ContainerProperties =
    ContainerProperties(*topicConfig.topics.toTypedArray()).apply {
      pollTimeout = topicConfig.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
      ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
      idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
      isSyncCommits = true
      syncCommitTimeout = Duration.ofSeconds(5)
      val executor = SimpleAsyncTaskExecutor("batch-kafka-vt-").apply { setVirtualThreads(true) }
      listenerTaskExecutor = executor
    }

  override fun stop() {
    if (!running.getAndSet(false)) {
      log.debug { "Consumer not running" }
      return
    }
    log.info { "Stopping batch consumer" }
    containers.forEach { container ->
      runCatching { container.stop() }
        .onFailure { e -> log.error(e) { "Error stopping container" } }
    }
    containers.clear()
  }

  override fun isRunning(): Boolean = running.get()
}

/**
 * Supervisor for batch auto-ack consumers.
 * Acknowledges the entire batch after [BatchConsumerAutoAck.consume] returns.
 */
class BatchConsumerAutoAckSupervisor<K : Any, V : Any>(
  private val consumer: BatchConsumerAutoAck<K, V>,
  config: ResolvedConsumerConfig,
  consumerFactory: ConsumerFactory<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  listenerConfig: ListenerConfig,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractBatchConsumerSupervisor<K, V>(consumer, config, consumerFactory, kafkaTemplate, listenerConfig, metrics) {
  override fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  ) {
    runBlocking { consumer.consume(records, failureHandler) }
    springAck?.acknowledge()
  }
}

/**
 * Supervisor for batch manual-ack consumers.
 * The user controls when offsets are committed by calling [Acknowledgment.acknowledge].
 */
class BatchConsumerManualAckSupervisor<K : Any, V : Any>(
  private val consumer: BatchConsumerManualAck<K, V>,
  config: ResolvedConsumerConfig,
  consumerFactory: ConsumerFactory<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  listenerConfig: ListenerConfig,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractBatchConsumerSupervisor<K, V>(consumer, config, consumerFactory, kafkaTemplate, listenerConfig, metrics) {
  override fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  ) {
    val kafkaFlowAck = Acknowledgment { springAck?.acknowledge() }
    runBlocking { consumer.consume(records, failureHandler, kafkaFlowAck) }
  }
}
