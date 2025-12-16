package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.KafkaPoller
import io.github.osoykan.kafkaflow.poller.PollerType
import io.github.osoykan.kafkaflow.poller.ReactorKafkaPoller
import io.github.osoykan.kafkaflow.poller.SpringKafkaPoller
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import reactor.kafka.receiver.ReceiverOptions

/**
 * Factory configuration for Kafka Flow.
 *
 * This abstraction hides Spring Kafka/Reactor Kafka implementation details.
 * Users only provide property maps and listener configuration.
 *
 * @property consumerProperties Kafka consumer properties (bootstrap.servers, group.id, etc.)
 * @property producerProperties Kafka producer properties (bootstrap.servers, acks, etc.)
 * @property listenerConfig Listener configuration (concurrency, poll timeout, virtual threads)
 * @property pollerType The underlying poller implementation to use
 * @property metrics Metrics implementation for observability
 * @property topicResolver Resolver for consumer topic configuration
 */
data class KafkaFlowFactoryConfig<K : Any, V : Any>(
  val consumerProperties: Map<String, Any>,
  val producerProperties: Map<String, Any>,
  val listenerConfig: ListenerConfig = ListenerConfig(),
  val pollerType: PollerType = PollerType.SPRING_KAFKA,
  val metrics: KafkaFlowMetrics = NoOpMetrics,
  val topicResolver: TopicResolver = TopicResolver()
)

/**
 * Factory for creating Kafka Flow components.
 *
 * This factory abstracts away the underlying Kafka client implementation
 * (Spring Kafka vs Reactor Kafka) and provides a unified API for creating
 * consumers, producers, and the consumer engine.
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
     * @param dispatcher Coroutine dispatcher for async operations
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

      // Create the appropriate poller based on config
      val supervisorFactory = when (config.pollerType) {
        PollerType.SPRING_KAFKA -> {
          val poller = SpringKafkaPoller<K, V>(
            consumerFactory = consumerFactory,
            listenerConfig = config.listenerConfig,
            dispatcher = dispatcher
          )
          FlowConsumerSupervisorFactory(
            poller = poller,
            kafkaTemplate = kafkaTemplate,
            topicResolver = config.topicResolver,
            listenerConfig = config.listenerConfig,
            metrics = config.metrics,
            consumerFactory = consumerFactory
          )
        }

        PollerType.REACTOR_KAFKA -> {
          val receiverOptions = ReceiverOptions.create<K, V>(config.consumerProperties)
          val poller = ReactorKafkaPoller(
            receiverOptions = receiverOptions,
            useVirtualThreads = config.listenerConfig.useVirtualThreads,
            dispatcher = dispatcher
          )
          FlowConsumerSupervisorFactory(
            poller = poller,
            kafkaTemplate = kafkaTemplate,
            topicResolver = config.topicResolver,
            listenerConfig = config.listenerConfig,
            metrics = config.metrics,
            consumerFactory = consumerFactory
          )
        }
      }

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

  /**
   * Gets the supervisor factory for advanced use cases.
   *
   * @return The internal ConsumerSupervisorFactory
   */
  fun supervisorFactory(): ConsumerSupervisorFactory<K, V> = supervisorFactory
}

/**
 * Internal supervisor factory that works with any KafkaPoller implementation.
 */
internal class FlowConsumerSupervisorFactory<K : Any, V : Any>(
  private val poller: KafkaPoller<K, V>,
  private val kafkaTemplate: KafkaTemplate<K, V>,
  private val topicResolver: TopicResolver,
  private val listenerConfig: ListenerConfig,
  private val metrics: KafkaFlowMetrics,
  private val consumerFactory: ConsumerFactory<K, V>
) : ConsumerSupervisorFactory<K, V> {
  override fun createSupervisors(consumers: List<Consumer<K, V>>): List<ConsumerSupervisor> = consumers.map { consumer ->
    val config = topicResolver.resolve(consumer)

    // Create FlowKafkaConsumer with the poller
    val flowConsumer = FlowKafkaConsumer.withPoller(poller)

    when (consumer) {
      is ConsumerAutoAck<K, V> -> {
        ConsumerAutoAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = flowConsumer,
          kafkaTemplate = kafkaTemplate,
          metrics = metrics
        )
      }

      is ConsumerManualAck<K, V> -> {
        // Manual ack still needs the Spring Kafka consumer factory for now
        val springFlowConsumer = FlowKafkaConsumer(
          consumerFactory = consumerFactory,
          listenerConfig = listenerConfig
        )
        ConsumerManualAckSupervisor(
          consumer = consumer,
          config = config,
          flowConsumer = springFlowConsumer,
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
