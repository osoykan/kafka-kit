package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.AckableRecord
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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
  protected val listenerConfig: ListenerConfig,
  protected val metrics: KafkaFlowMetrics = NoOpMetrics,
  final override val consumerName: String
) : ConsumerSupervisor {
  protected val log = KotlinLogging.logger("Supervisor[$consumerName]")

  protected val scope = CoroutineScope(
    Dispatchers.IO + SupervisorJob() + CoroutineName(consumerName)
  )

  private var running = false

  protected val retryProcessor = RetryableProcessor(
    kafkaTemplate = kafkaTemplate,
    policy = config.retry,
    classifier = config.classifier,
    metrics = metrics,
    consumerName = consumerName
  )

  override val topics: List<String>
    get() = config.topic.topics + config.retryTopic

  override fun start() {
    if (running) {
      log.warn { "Consumer already running" }
      return
    }

    log.info { "Starting consumer for topics: [${config.topic.displayName}]" }
    running = true

    launchConsumer(config.topic)
    launchConsumer(TopicConfig(name = config.retryTopic))
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private fun launchConsumer(topicConfig: TopicConfig): Job {
    val concurrency = topicConfig.effectiveConcurrency(listenerConfig.concurrency)
    log.info { "Starting consumer for [${topicConfig.displayName}] with processing concurrency: $concurrency" }

    return scope.launch {
      flowConsumer
        .consume(topicConfig)
        .catch { e ->
          log.error(e) { "Stream error on topics: [${topicConfig.displayName}]" }
          metrics.recordProcessingFailure(topicConfig.displayName, consumerName, e)
        }.flatMapMerge(concurrency) { ackRecord ->
          flow { emit(processRecord(ackRecord, ackRecord.record.topic())) }
        }.collect()
    }
  }

  private suspend fun processRecord(ackRecord: AckableRecord<K, V>, topic: String) {
    metrics.recordConsumed(topic, consumerName, ackRecord.record.partition())

    val startTime = System.nanoTime()
    val result = handleRecord(ackRecord)
    val duration = (System.nanoTime() - startTime).nanoseconds

    logResult(result, topic, duration)
  }

  /**
   * Process a single record. Subclasses implement specific consumer invocation.
   */
  protected abstract suspend fun handleRecord(ackRecord: AckableRecord<K, V>): ProcessingResult<*>

  private fun logResult(result: ProcessingResult<*>, topic: String, duration: Duration) {
    when (result) {
      is ProcessingResult.Success -> {
        metrics.recordProcessingSuccess(topic, consumerName, duration)
        log.debug { "Successfully processed record from $topic" }
      }

      is ProcessingResult.SentToRetryTopic -> {
        log.warn { "Record sent to retry topic: ${result.topic} (attempt ${result.attempt})" }
      }

      is ProcessingResult.SentToDlt -> {
        log.error { "Record sent to DLT: ${result.topic} - ${result.reason}" }
      }

      is ProcessingResult.Expired -> {
        log.warn { "Record expired and sent to DLT: ${result.topic} - ${result.reason}" }
      }

      is ProcessingResult.Failed -> {
        log.error(result.exception) { "Failed to process/send record" }
        metrics.recordProcessingFailure(topic, consumerName, result.exception)
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
 * Spring Kafka handles commits automatically based on CommitStrategy.
 */
class ConsumerAutoAckSupervisor<K : Any, V : Any>(
  private val consumer: ConsumerAutoAck<K, V>,
  config: ResolvedConsumerConfig,
  flowConsumer: FlowKafkaConsumer<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  listenerConfig: ListenerConfig,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractConsumerSupervisor<K, V>(config, flowConsumer, kafkaTemplate, listenerConfig, metrics, consumer.consumerName) {
  override suspend fun handleRecord(ackRecord: AckableRecord<K, V>): ProcessingResult<*> =
    retryProcessor.process(ackRecord.record) { rec -> consumer.consume(rec) }
}

/**
 * Supervisor for manual-ack consumers.
 * User controls when to acknowledge via the Acknowledgment handle.
 */
class ConsumerManualAckSupervisor<K : Any, V : Any>(
  private val consumer: ConsumerManualAck<K, V>,
  config: ResolvedConsumerConfig,
  flowConsumer: FlowKafkaConsumer<K, V>,
  kafkaTemplate: KafkaTemplate<K, V>,
  listenerConfig: ListenerConfig,
  metrics: KafkaFlowMetrics = NoOpMetrics
) : AbstractConsumerSupervisor<K, V>(config, flowConsumer, kafkaTemplate, listenerConfig, metrics, consumer.consumerName) {
  override suspend fun handleRecord(ackRecord: AckableRecord<K, V>): ProcessingResult<*> {
    val ack = Acknowledgment { ackRecord.acknowledge() }
    return retryProcessor.process(ackRecord.record) { rec -> consumer.consume(rec, ack) }
  }
}
