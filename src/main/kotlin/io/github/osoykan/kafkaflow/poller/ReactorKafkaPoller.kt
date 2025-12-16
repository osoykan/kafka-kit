package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.TopicConfig
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

private const val VIRTUAL_THREADS_PROPERTY = "reactor.schedulers.defaultBoundedElasticOnVirtualThreads"

/**
 * Reactor Kafka based poller using KafkaReceiver.
 *
 * Features:
 * - Non-blocking reactive polling via Project Reactor
 * - Auto-acknowledgment (`poll`) or manual acknowledgment (`pollWithAck`)
 * - Virtual Threads enabled by default via `Schedulers.boundedElastic()` (JDK 21+)
 *
 * ## Virtual Threads (Default: enabled)
 *
 * Virtual Threads are enabled by default. This is done by setting:
 * ```
 * -Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true
 * ```
 *
 * To disable, set `useVirtualThreads = false` in the constructor.
 *
 * Uses `.asFlow()` from kotlinx-coroutines-reactive for seamless Flux to Flow conversion.
 * Cancellation of the Flow automatically disposes the underlying Flux subscription.
 *
 * @param receiverOptions Base Reactor Kafka receiver options
 * @param useVirtualThreads Enable Virtual Threads for Reactor schedulers (default: true)
 * @param dispatcher Coroutine dispatcher for flow processing (default: Dispatchers.IO)
 */
class ReactorKafkaPoller<K : Any, V : Any>(
  private val receiverOptions: ReceiverOptions<K, V>,
  useVirtualThreads: Boolean = true,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : KafkaPoller<K, V> {
  private val stopped = AtomicBoolean(false)

  init {
    if (useVirtualThreads) {
      enableVirtualThreads()
    }
  }

  companion object {
    private val virtualThreadsEnabled = AtomicBoolean(false)

    /**
     * Enables Virtual Threads for Reactor's boundedElastic scheduler.
     * This is called automatically when creating a ReactorKafkaPoller with useVirtualThreads=true.
     */
    fun enableVirtualThreads() {
      if (virtualThreadsEnabled.compareAndSet(false, true)) {
        System.setProperty(VIRTUAL_THREADS_PROPERTY, "true")
        logger.info { "ReactorKafkaPoller: Virtual Threads enabled for Reactor schedulers" }
      }
    }
  }

  /**
   * Consumes messages with auto-acknowledgment.
   *
   * Uses `receiveAutoAck()` which commits offsets automatically based on
   * `commitBatchSize` and `commitInterval` in ReceiverOptions.
   */
  override fun poll(topic: TopicConfig): Flow<ConsumerRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }

    val topicOptions = createTopicOptions(topic)
    val receiver = KafkaReceiver.create(topicOptions)

    logger.info { "ReactorKafkaPoller: Starting auto-ack consumer for topic: ${topic.name}" }

    // receiveAutoAck() returns Flux<Flux<ConsumerRecord>>
    // Flatten with concatMap BEFORE converting to Flow (like flowIn pattern)
    // groupBy key/partition then flatMap to preserve ordering per key
    return receiver
      .receiveAutoAck()
      .concatMap { partitionFlux ->
        partitionFlux
          .groupBy { record -> record.key() ?: record.partition() }
          .flatMap { it }
      }.asFlow()
      .flowOn(dispatcher)
  }

  /**
   * Consumes messages with manual acknowledgment.
   *
   * Uses `receive()` which returns ReceiverRecords. You must call `acknowledge()`
   * on each record after successful processing to commit the offset.
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
  override fun pollWithAck(topic: TopicConfig): Flow<AckableRecord<K, V>> {
    if (stopped.get()) {
      return emptyFlow()
    }

    val topicOptions = createTopicOptions(topic)
    val receiver = KafkaReceiver.create(topicOptions)

    logger.info { "ReactorKafkaPoller: Starting manual-ack consumer for topic: ${topic.name}" }

    // receive() returns Flux<ReceiverRecord> with manual ack control
    return receiver
      .receive()
      .asFlow()
      .map { receiverRecord ->
        AckableRecord(
          record = receiverRecord,
          acknowledge = { receiverRecord.receiverOffset().acknowledge() }
        )
      }.flowOn(dispatcher)
  }

  private fun createTopicOptions(topic: TopicConfig): ReceiverOptions<K, V> = receiverOptions
    .subscription(listOf(topic.name))
    .apply {
      topic.pollTimeout?.let { timeout ->
        pollTimeout(timeout.toJavaDuration())
      }
    }.schedulerSupplier { Schedulers.boundedElastic() }

  override fun stop() {
    if (stopped.compareAndSet(false, true)) {
      logger.info { "ReactorKafkaPoller: Stopped" }
    }
  }

  override fun isStopped(): Boolean = stopped.get()
}
