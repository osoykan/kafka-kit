package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.BackpressureController
import io.github.osoykan.kafkaflow.poller.ContainerConfiguration
import io.github.osoykan.kafkaflow.poller.ContainerRef
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.nanoseconds

/**
 * Abstract base supervisor for batch consumers.
 *
 * Manages [ConcurrentMessageListenerContainer] lifecycle with [BatchAcknowledgingMessageListener].
 * No flow pipeline, no [OrderedCommitter] - the entire batch is acknowledged after processing.
 * No automatic retry - the user handles per-record failures via [FailureHandler].
 *
 * Uses a channel to bridge the Spring Kafka listener thread to a coroutine, following the same
 * pattern as [io.github.osoykan.kafkaflow.poller.SpringKafkaPoller]. The channel's
 * [trySendBlocking] provides natural backpressure by blocking the listener thread when the
 * channel is full, and [BackpressureController] adds container pause/resume on top.
 *
 * Subclasses only differ in how they invoke the consumer and handle acknowledgment.
 */
private const val BATCH_CHANNEL_CAPACITY = 4

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

  private val scope = CoroutineScope(
    Dispatchers.IO + SupervisorJob() + CoroutineName(consumer.consumerName)
  )

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
   * Invoked inside the batch processing coroutine.
   * Subclasses bridge to the consumer's suspend function and handle acknowledgment.
   */
  protected abstract suspend fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  )

  /**
   * Envelope sent through the channel from the listener thread to the processing coroutine.
   */
  private data class BatchEnvelope<K : Any, V : Any>(
    val records: List<ConsumerRecord<K, V>>,
    val ack: org.springframework.kafka.support.Acknowledgment?
  )

  private fun startContainer(topicConfig: TopicConfig) {
    val containerRef = ContainerRef<K, V>()
    val batchChannel = Channel<BatchEnvelope<K, V>>(capacity = BATCH_CHANNEL_CAPACITY)

    val backpressure = BackpressureController(
      containerProvider = { containerRef.get() },
      config = listenerConfig.backpressure,
      bufferCapacity = BATCH_CHANNEL_CAPACITY,
      topicName = topicConfig.displayName
    )

    // Launch coroutine to process batches from the channel
    scope.launch {
      for (envelope in batchChannel) {
        val startTime = System.nanoTime()
        try {
          metrics.recordBatchConsumed(topicConfig.displayName, consumerName, envelope.records.size)
          onBatch(envelope.records, failureHandler, envelope.ack)
          val duration = (System.nanoTime() - startTime).nanoseconds
          metrics.recordBatchProcessingSuccess(topicConfig.displayName, consumerName, envelope.records.size, duration)
        } catch (e: Exception) {
          log.error(e) { "Batch processing failed for topic: ${topicConfig.displayName}" }
          metrics.recordBatchProcessingFailure(topicConfig.displayName, consumerName, envelope.records.size, e)
        } finally {
          backpressure.onBufferConsume()
        }
      }
    }

    val containerProps = ContainerConfiguration.createContainerProperties(topicConfig, listenerConfig)
    containerProps.setMessageListener(
      BatchAcknowledgingMessageListener<K, V> { records, ack ->
        batchChannel
          .trySendBlocking(BatchEnvelope(records, ack))
          .onSuccess { backpressure.onBufferAdd() }
          .onFailure { e ->
            if (e !is CancellationException) {
              log.error(e) { "Failed to send batch to channel for ${topicConfig.displayName}" }
            }
          }
      }
    )

    val container = ContainerConfiguration.createContainer(consumerFactory, containerProps, topicConfig, listenerConfig)
    containerRef.set(container)
    containers.add(container)
    container.start()
    log.info {
      "Started batch container for [${topicConfig.displayName}], " +
        "partitions: ${container.concurrency}, " +
        "backpressure: ${if (listenerConfig.backpressure.enabled) "enabled" else "disabled"}"
    }
  }

  override fun stop() {
    if (!running.getAndSet(false)) {
      log.debug { "Consumer not running" }
      return
    }
    log.info { "Stopping batch consumer" }
    scope.cancel()
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
  override suspend fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  ) {
    consumer.consume(records, failureHandler)
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
  override suspend fun onBatch(
    records: List<ConsumerRecord<K, V>>,
    failureHandler: FailureHandler<K, V>,
    springAck: org.springframework.kafka.support.Acknowledgment?
  ) {
    val kafkaFlowAck = Acknowledgment { springAck?.acknowledge() }
    consumer.consume(records, failureHandler, kafkaFlowAck)
  }
}
