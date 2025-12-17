package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.AckMode
import kotlinx.coroutines.*
import org.springframework.kafka.core.*

/**
 * Factory configuration for Kafka Flow.
 *
 * Users only provide property maps and listener configuration.
 * The factory handles all Spring Kafka setup internally.
 *
 * @property consumerProperties Kafka consumer properties (bootstrap.servers, group.id, etc.)
 * @property producerProperties Kafka producer properties (bootstrap.servers, acks, etc.)
 * @property listenerConfig Listener configuration (concurrency, poll timeout, virtual threads)
 * @property metrics Metrics implementation for observability
 * @property topicResolver Resolver for consumer topic configuration
 */
data class KafkaFlowFactoryConfig<K : Any, V : Any>(
  val consumerProperties: Map<String, Any>,
  val producerProperties: Map<String, Any>,
  val listenerConfig: ListenerConfig = ListenerConfig(),
  val metrics: KafkaFlowMetrics = NoOpMetrics,
  val topicResolver: TopicResolver = TopicResolver()
)

/**
 * Factory for creating Kafka Flow components.
 *
 * This factory handles all Spring Kafka setup internally and provides
 * a unified API for creating consumers, producers, and the consumer engine.
 *
 * ## Usage
 *
 * ```kotlin
 * val factory = KafkaFlowFactory.create(
 *   KafkaFlowConfig(
 *     consumerProperties = mapOf(
 *       "bootstrap.servers" to "localhost:9092",
 *       "group.id" to "my-group",
 *       "key.deserializer" to StringDeserializer::class.java,
 *       "value.deserializer" to StringDeserializer::class.java
 *     ),
 *     producerProperties = mapOf(
 *       "bootstrap.servers" to "localhost:9092",
 *       "key.serializer" to StringSerializer::class.java,
 *       "value.serializer" to StringSerializer::class.java
 *     )
 *   )
 * )
 *
 * val engine = factory.createConsumerEngine(consumers)
 * engine.start()
 * ```
 */
class KafkaFlowFactory<K : Any, V : Any> private constructor(
  private val config: KafkaFlowFactoryConfig<K, V>,
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val supervisorFactory: ConsumerSupervisorFactory<K, V>
) {
  companion object {
    /**
     * Creates a KafkaFlowFactory with the given configuration.
     *
     * @param config Kafka Flow configuration
     * @param dispatcher Coroutine dispatcher for flow emission (default: Dispatchers.IO)
     * @return A new KafkaFlowFactory instance
     */
    fun <K : Any, V : Any> create(
      config: KafkaFlowFactoryConfig<K, V>,
      dispatcher: CoroutineDispatcher = Dispatchers.IO
    ): KafkaFlowFactory<K, V> {
      // Create Spring Kafka factories internally
      val consumerFactory = DefaultKafkaConsumerFactory<K, V>(config.consumerProperties)
      val producerFactory = DefaultKafkaProducerFactory<K, V>(config.producerProperties)
      val kafkaTemplate = KafkaTemplate(producerFactory)

      val supervisorFactory = FlowConsumerSupervisorFactory(
        kafkaTemplate = kafkaTemplate,
        topicResolver = config.topicResolver,
        listenerConfig = config.listenerConfig,
        metrics = config.metrics,
        consumerFactory = consumerFactory,
        dispatcher = dispatcher
      )

      return KafkaFlowFactory(config, kafkaTemplate, supervisorFactory)
    }
  }

  /**
   * Creates a consumer engine with the given consumers.
   *
   * @param consumers List of consumers to register
   * @param enabled Whether the engine is enabled
   * @return A configured ConsumerEngine
   */
  fun createConsumerEngine(
    consumers: List<Consumer<K, V>>,
    enabled: Boolean = true
  ): ConsumerEngine<K, V> = ConsumerEngine(
    consumers = consumers,
    supervisorFactory = supervisorFactory,
    metrics = config.metrics,
    enabled = enabled
  )

  /**
   * Gets the KafkaTemplate for producing messages.
   *
   * @return The internal KafkaTemplate
   */
  fun kafkaTemplate(): KafkaTemplate<K, V> = kafkaTemplate
}

/**
 * Internal supervisor factory using Spring Kafka.
 */
internal class FlowConsumerSupervisorFactory<K : Any, V : Any>(
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val topicResolver: TopicResolver,
  private val listenerConfig: ListenerConfig,
  private val metrics: KafkaFlowMetrics,
  private val consumerFactory: ConsumerFactory<K, V>,
  private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : ConsumerSupervisorFactory<K, V> {
  override fun createSupervisors(consumers: List<Consumer<K, V>>): List<ConsumerSupervisor> = consumers.map { consumer ->
    val config = topicResolver.resolve(consumer)

    when (consumer) {
      is ConsumerAutoAck<K, V> -> {
        val flowConsumer = FlowKafkaConsumer(consumerFactory, listenerConfig, AckMode.AUTO, dispatcher = dispatcher)
        ConsumerAutoAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = flowConsumer,
          kafkaTemplate = kafkaTemplate,
          listenerConfig = listenerConfig,
          metrics = metrics
        )
      }

      is ConsumerManualAck<K, V> -> {
        val flowConsumer = FlowKafkaConsumer(consumerFactory, listenerConfig, AckMode.MANUAL, dispatcher = dispatcher)
        ConsumerManualAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = flowConsumer,
          kafkaTemplate = kafkaTemplate,
          listenerConfig = listenerConfig,
          metrics = metrics
        )
      }

      else -> {
        error("Unknown consumer type: ${consumer::class.simpleName}")
      }
    }
  }
}
