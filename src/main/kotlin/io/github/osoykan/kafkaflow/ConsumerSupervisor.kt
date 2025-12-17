package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.AckableRecord
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.catch
import org.springframework.kafka.core.KafkaTemplate
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

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
    get() = listOf(config.topic.name, config.retryTopic)

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
  protected fun startRetryTopicConsumer(): Job = launchRetryConsumer(TopicConfig(name = config.retryTopic))

  /**
   * Launch retry topic consumer. Subclasses override for specific handling.
   */
  protected abstract fun launchRetryConsumer(topicConfig: TopicConfig): Job

  /**
   * Handle the processing result with logging and metrics.
   * Common result handling extracted to avoid duplication.
   *
   * @return true if the record was successfully handled (success, retry, or DLT)
   */
  protected fun handleProcessingResult(
    ackFn: () -> Unit,
    result: ProcessingResult<*>,
    topic: String,
    duration: Duration
  ): Boolean = when (result) {
    is ProcessingResult.Success -> {
      metrics.recordProcessingSuccess(topic, consumerName, duration)
      log.debug { "Successfully processed record from $topic" }
      true
    }

    is ProcessingResult.SentToRetryTopic -> {
      ackFn() // Acknowledge offset since sent to retry topic
      log.warn { "Record sent to retry topic: ${result.topic} (attempt ${result.attempt})" }
      true // Considered handled
    }

    is ProcessingResult.SentToDlt -> {
      ackFn() // Acknowledge offset since sent to DLT
      log.error { "Record sent to DLT: ${result.topic} - ${result.reason}" }
      true // Considered handled (in DLT now)
    }

    is ProcessingResult.Expired -> {
      ackFn() // Acknowledge offset since expired
      log.warn { "Record expired and sent to DLT: ${result.topic} - ${result.reason}" }
      true // Considered handled
    }

    is ProcessingResult.Failed -> {
      log.error(result.exception) { "Failed to process/send record" }
      metrics.recordProcessingFailure(topic, consumerName, result.exception)
      false
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
 * Uses CommitStrategy-based ack mode. Spring Kafka handles commits automatically.
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
      }.collect { ackRecord -> handleRecord(ackRecord, config.topic.name) }
  }

  override fun launchRetryConsumer(topicConfig: TopicConfig): Job = scope.launch {
    flowConsumer
      .consume(topicConfig)
      .catch { e ->
        log.error(e) { "Stream error on retry topic: ${topicConfig.name}" }
        metrics.recordProcessingFailure(topicConfig.name, consumerName, e)
      }.collect { ackRecord -> handleRecord(ackRecord, topicConfig.name) }
  }

  /**
   * Process record. Spring Kafka handles commits based on CommitStrategy.
   */
  private suspend fun handleRecord(ackRecord: AckableRecord<K, V>, topic: String) {
    metrics.recordConsumed(topic, consumerName, ackRecord.record.partition())

    val startTime = System.nanoTime()
    val result = retryProcessor.process(ackRecord.record) { rec -> consumer.consume(rec) }
    val duration = (System.nanoTime() - startTime).nanoseconds

    handleProcessingResult({ ackRecord.acknowledge() }, result, topic, duration)
  }
}

/**
 * Supervisor for manual-ack consumers.
 *
 * Uses MANUAL_IMMEDIATE mode - user controls when to acknowledge.
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
      .consume(config.topic)
      .catch { e ->
        log.error(e) { "Stream error on main topic: ${config.topic.name}" }
        metrics.recordProcessingFailure(config.topic.name, consumerName, e)
      }.collect { ackRecord -> handleRecord(ackRecord, config.topic.name) }
  }

  override fun launchRetryConsumer(topicConfig: TopicConfig): Job = scope.launch {
    flowConsumer
      .consume(topicConfig)
      .catch { e ->
        log.error(e) { "Stream error on retry topic: ${topicConfig.name}" }
        metrics.recordProcessingFailure(topicConfig.name, consumerName, e)
      }.collect { ackRecord -> handleRecord(ackRecord, topicConfig.name) }
  }

  /**
   * Process record - user controls acknowledgment via SpringAcknowledgmentAdapter.
   */
  private suspend fun handleRecord(ackRecord: AckableRecord<K, V>, topic: String) {
    metrics.recordConsumed(topic, consumerName, ackRecord.record.partition())

    val startTime = System.nanoTime()
    val ack = Acknowledgment { ackRecord.acknowledge() }
    val result = retryProcessor.process(ackRecord.record) { rec -> consumer.consume(rec, ack) }
    val duration = (System.nanoTime() - startTime).nanoseconds

    handleProcessingResult({ }, result, topic, duration)
  }
}
