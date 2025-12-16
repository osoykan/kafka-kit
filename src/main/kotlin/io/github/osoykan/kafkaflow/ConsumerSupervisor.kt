package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import kotlin.time.Duration.Companion.nanoseconds

private val logger = KotlinLogging.logger {}

/**
 * Interface for consumer supervisors.
 * Supervisors manage the lifecycle of consumers and handle all internal concerns
 * like retry, DLT, and metrics.
 */
interface ConsumerSupervisor {
  /** The name of the consumer being supervised. */
  val consumerName: String

  /** Topics this supervisor is consuming from (main + retry). */
  val topics: List<String>

  /** Start the consumer. */
  fun start()

  /** Stop the consumer gracefully. */
  fun stop()

  /** Check if the consumer is running. */
  fun isRunning(): Boolean
}

/**
 * Factory for creating consumer supervisors.
 */
interface ConsumerSupervisorFactory<K : Any, V : Any> {
  /** Creates supervisors for a list of consumers. */
  fun createSupervisors(consumers: List<Consumer<K, V>>): List<ConsumerSupervisor>
}

/**
 * Abstract base supervisor that handles common lifecycle and flow management.
 *
 * INTERNAL: Handles all retry logic, metrics, and error handling.
 * Consumer implementations only need to implement consume() - nothing else.
 */
abstract class AbstractConsumerSupervisor<K : Any, V : Any>(
  protected val config: ResolvedConsumerConfig,
  protected val flowConsumer: FlowKafkaConsumer<K, V>,
  protected val kafkaTemplate: KafkaTemplate<K, V>,
  protected val metrics: KafkaFlowMetrics = NoOpMetrics,
  final override val consumerName: String
) : ConsumerSupervisor {
  protected val log = KotlinLogging.logger("Supervisor[$consumerName]")

  protected val scope = CoroutineScope(
    Dispatchers.IO + SupervisorJob() + CoroutineName(consumerName)
  )

  private var running = false

  // INTERNAL: RetryableProcessor with all features - consumer doesn't know about this
  protected val retryProcessor = RetryableProcessor(
    kafkaTemplate = kafkaTemplate,
    policy = config.retry,
    classifier = config.classifier,
    metrics = metrics,
    consumerName = consumerName
  )

  override val topics: List<String>
    get() = listOfNotNull(config.topic.name, config.retryTopic)

  override fun start() {
    if (running) {
      log.warn { "Consumer already running" }
      return
    }

    log.info { "Starting consumer for topic: ${config.topic.name}" }
    running = true

    startMainTopicConsumer()
    startRetryTopicConsumer()
  }

  /**
   * Start consuming from the main topic. Subclasses implement specific flow handling.
   */
  protected abstract fun startMainTopicConsumer(): Job

  /**
   * Start consuming from the retry topic. Subclasses implement specific flow handling.
   */
  protected fun startRetryTopicConsumer(): Job? {
    val retryTopic = config.retryTopic ?: return null
    return launchRetryConsumer(TopicConfig(name = retryTopic))
  }

  /**
   * Launch retry topic consumer. Subclasses override for specific handling.
   */
  protected abstract fun launchRetryConsumer(topicConfig: TopicConfig): Job

  /**
   * Handle the processing result with logging and metrics.
   * Common result handling extracted to avoid duplication.
   */
  protected fun handleProcessingResult(
    result: ProcessingResult<*>,
    topic: String,
    duration: kotlin.time.Duration
  ): Boolean {
    when (result) {
      is ProcessingResult.Success -> {
        metrics.recordProcessingSuccess(topic, consumerName, duration)
        log.debug { "Successfully processed record from $topic" }
        return true
      }

      is ProcessingResult.SentToRetryTopic -> {
        log.warn { "Record sent to retry topic: ${result.topic} (attempt ${result.attempt})" }
        return true // Considered handled
      }

      is ProcessingResult.SentToDlt -> {
        log.error { "Record sent to DLT: ${result.topic} - ${result.reason}" }
        return true // Considered handled (in DLT now)
      }

      is ProcessingResult.Expired -> {
        log.warn { "Record expired and sent to DLT: ${result.topic} - ${result.reason}" }
        return true // Considered handled
      }

      is ProcessingResult.Failed -> {
        log.error(result.exception) { "Failed to process/send record" }
        metrics.recordProcessingFailure(topic, consumerName, result.exception)
        return false
      }
    }
  }

  override fun stop() {
    if (!running) {
      log.debug { "Consumer not running" }
      return
    }

    log.info { "Stopping consumer" }
    running = false
    scope.cancel()
    flowConsumer.stop()
  }

  override fun isRunning(): Boolean = running
}

/**
 * Supervisor for auto-ack consumers.
 *
 * INTERNAL: Handles all retry logic, metrics, and error handling.
 * Consumer only implements consume() - nothing else.
 */
class ConsumerAutoAckSupervisor<K : Any, V : Any>(
  private val consumer: ConsumerAutoAck<K, V>,
  config: ResolvedConsumerConfig,
  flowConsumer: FlowKafkaConsumer<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractConsumerSupervisor<K, V>(config, flowConsumer, kafkaTemplate, metrics, consumer.consumerName) {
  override fun startMainTopicConsumer(): Job = scope.launch {
    flowConsumer
      .consume(config.topic)
      .catch { e ->
        log.error(e) { "Stream error on main topic: ${config.topic.name}" }
        metrics.recordProcessingFailure(config.topic.name, consumerName, e)
      }.collect { record -> handleRecord(record, config.topic.name) }
  }

  override fun launchRetryConsumer(topicConfig: TopicConfig): Job = scope.launch {
    flowConsumer
      .consume(topicConfig)
      .catch { e ->
        log.error(e) { "Stream error on retry topic: ${topicConfig.name}" }
        metrics.recordProcessingFailure(topicConfig.name, consumerName, e)
      }.collect { record -> handleRecord(record, topicConfig.name) }
  }

  /**
   * INTERNAL: All retry logic and metrics hidden here.
   * Consumer just sees consume() called - nothing else.
   */
  private suspend fun handleRecord(record: ConsumerRecord<K, V>, topic: String) {
    metrics.recordConsumed(topic, consumerName, record.partition())

    val startTime = System.nanoTime()
    val result = retryProcessor.process(record) { rec ->
      consumer.consume(rec) // Call the clean consumer
    }
    val duration = (System.nanoTime() - startTime).nanoseconds

    handleProcessingResult(result, topic, duration)
  }
}

/**
 * Supervisor for manual-ack consumers.
 *
 * INTERNAL: Handles all retry logic, metrics, and error handling.
 * Consumer implements consume() with acknowledgment control.
 */
class ConsumerManualAckSupervisor<K : Any, V : Any>(
  private val consumer: ConsumerManualAck<K, V>,
  config: ResolvedConsumerConfig,
  flowConsumer: FlowKafkaConsumer<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractConsumerSupervisor<K, V>(config, flowConsumer, kafkaTemplate, metrics, consumer.consumerName) {
  override fun startMainTopicConsumer(): Job = scope.launch {
    flowConsumer
      .consumeWithAck(config.topic)
      .catch { e ->
        log.error(e) { "Stream error on main topic: ${config.topic.name}" }
        metrics.recordProcessingFailure(config.topic.name, consumerName, e)
      }.collect { ackRecord -> handleRecord(ackRecord, config.topic.name) }
  }

  override fun launchRetryConsumer(topicConfig: TopicConfig): Job = scope.launch {
    flowConsumer
      .consumeWithAck(topicConfig)
      .catch { e ->
        log.error(e) { "Stream error on retry topic: ${topicConfig.name}" }
        metrics.recordProcessingFailure(topicConfig.name, consumerName, e)
      }.collect { ackRecord -> handleRecord(ackRecord, topicConfig.name) }
  }

  /**
   * INTERNAL: All retry logic and metrics hidden here.
   */
  private suspend fun handleRecord(ackRecord: AcknowledgingRecord<K, V>, topic: String) {
    metrics.recordConsumed(topic, consumerName, ackRecord.record.partition())

    val startTime = System.nanoTime()
    val wrappedAck = SpringAcknowledgmentAdapter(ackRecord.acknowledgment)

    val result = retryProcessor.process(ackRecord.record) { rec ->
      consumer.consume(rec, wrappedAck) // Call the consumer with ack
    }
    val duration = (System.nanoTime() - startTime).nanoseconds

    val handled = handleProcessingResult(result, topic, duration)

    // For manual ack, acknowledge after successful handling or when sent to retry/DLT
    if (handled && result !is ProcessingResult.Failed) {
      ackRecord.acknowledgment.acknowledge()
    }
    // If Failed and not handled, don't acknowledge - message will be redelivered
  }
}

/**
 * Default implementation of ConsumerSupervisorFactory.
 */
class DefaultConsumerSupervisorFactory<K : Any, V : Any>(
  private val consumerFactory: ConsumerFactory<K, V>,
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val topicResolver: TopicResolver,
  private val listenerConfig: ListenerConfig = ListenerConfig(),
  private val metrics: KafkaFlowMetrics = NoOpMetrics
) : ConsumerSupervisorFactory<K, V> {
  override fun createSupervisors(consumers: List<Consumer<K, V>>): List<ConsumerSupervisor> = consumers.map { consumer ->
    val config = topicResolver.resolve(consumer)

    val flowConsumer = FlowKafkaConsumer<K, V>(
      consumerFactory = consumerFactory,
      listenerConfig = listenerConfig
    )

    when (consumer) {
      is ConsumerAutoAck<K, V> -> {
        logger.debug { "Creating AutoAck supervisor for: ${consumer.consumerName}" }
        ConsumerAutoAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = flowConsumer,
          kafkaTemplate = kafkaTemplate,
          metrics = metrics
        )
      }

      is ConsumerManualAck<K, V> -> {
        logger.debug { "Creating ManualAck supervisor for: ${consumer.consumerName}" }
        ConsumerManualAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = flowConsumer,
          kafkaTemplate = kafkaTemplate,
          metrics = metrics
        )
      }

      else -> {
        error("Unknown consumer type: ${consumer::class.simpleName}")
      }
    }
  }
}
