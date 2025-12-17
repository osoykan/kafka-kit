package io.github.osoykan.ktor.springkafka

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration DSL for Spring Kafka plugin.
 *
 * Supports multiple consumer and producer factories for different Kafka clusters or configurations.
 *
 * ## Basic Example (Single Cluster)
 *
 * ```kotlin
 * install(SpringKafka) {
 *   bootstrapServers = "localhost:9092"
 *   groupId = "my-consumer-group"
 *
 *   consumerPackages("com.example.consumers")
 *
 *   consumer {
 *     concurrency = 4
 *     pollTimeout = 1.seconds
 *   }
 *
 *   producer {
 *     acks = "all"
 *     retries = 3
 *   }
 * }
 * ```
 *
 * ## Multi-Cluster Example
 *
 * ```kotlin
 * install(SpringKafka) {
 *   // Default configuration (used by default factory)
 *   bootstrapServers = "cluster-a:9092"
 *   groupId = "my-app"
 *
 *   consumerPackages("com.example.consumers")
 *
 *   // Additional consumer factory for cluster B
 *   consumerFactory("clusterB") {
 *     bootstrapServers = "cluster-b:9092"
 *     groupId = "my-app-cluster-b"
 *     concurrency = 2
 *     valueDeserializer = AvroDeserializer::class
 *   }
 *
 *   // Additional producer for analytics cluster
 *   producerFactory("analytics") {
 *     bootstrapServers = "analytics-cluster:9092"
 *     acks = "1"
 *     compression = "snappy"
 *   }
 * }
 * ```
 *
 * ## Using Named Factories in Consumers
 *
 * ```kotlin
 * @Component
 * open class MyConsumer {
 *   // Uses default factory
 *   @KafkaListener(topics = ["orders"])
 *   open suspend fun consumeOrders(record: ConsumerRecord<String, Order>) { }
 *
 *   // Uses named factory "clusterB"
 *   @KafkaListener(topics = ["events"], containerFactory = "clusterBKafkaListenerContainerFactory")
 *   open suspend fun consumeFromClusterB(record: ConsumerRecord<String, Event>) { }
 * }
 * ```
 *
 * ## Using Named Producers
 *
 * ```kotlin
 * // Get default template
 * val defaultTemplate = application.kafkaTemplate<String, Order>()
 *
 * // Get named template for analytics
 * val analyticsTemplate = application.kafkaTemplate<String, AnalyticsEvent>("analytics")
 * ```
 */
class SpringKafkaConfig {
  // ─────────────────────────────────────────────────────────────────────────────
  // Dependency Resolution
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * External dependency resolver for beans not found in Spring context.
   *
   * When Spring needs to autowire a dependency into a @KafkaListener consumer,
   * it will first look in its own context. If not found, it falls back to this
   * resolver, which can query external DI containers like Koin.
   *
   * ## Auto-detection (Default)
   *
   * By default, the plugin auto-detects Koin if installed. You only need to set
   * this explicitly if:
   * - You're using a different DI container
   * - You want to chain multiple resolvers
   * - You want to disable fallback resolution
   *
   * ## Examples
   *
   * ```kotlin
   * // Use Koin (auto-detected by default)
   * install(SpringKafka) {
   *   // dependencyResolver is auto-configured from Koin
   * }
   *
   * // Explicit Koin resolver
   * install(SpringKafka) {
   *   dependencyResolver = KoinDependencyResolver.fromApplication(application)
   * }
   *
   * // Custom resolver
   * install(SpringKafka) {
   *   dependencyResolver = MyCustomResolver(myContainer)
   * }
   *
   * // Chain multiple resolvers
   * install(SpringKafka) {
   *   dependencyResolver = CompositeDependencyResolver(
   *     KoinDependencyResolver.fromApplication(application),
   *     MyFallbackResolver()
   *   )
   * }
   *
   * // Disable fallback (Spring-only)
   * install(SpringKafka) {
   *   dependencyResolver = NoOpDependencyResolver
   * }
   * ```
   */
  var dependencyResolver: DependencyResolver? = null

  // ─────────────────────────────────────────────────────────────────────────────
  // Default Configuration (used by default factories)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Default Kafka bootstrap servers.
   * Example: "localhost:9092" or "broker1:9092,broker2:9092"
   */
  var bootstrapServers: String = "localhost:9092"

  /**
   * Default consumer group ID.
   */
  var groupId: String = "ktor-spring-kafka"

  /**
   * Default key deserializer class for consumers.
   */
  var keyDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class

  /**
   * Default value deserializer class for consumers.
   */
  var valueDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class

  /**
   * Default key serializer class for producers.
   */
  var keySerializer: KClass<out Serializer<*>> = StringSerializer::class

  /**
   * Default value serializer class for producers.
   */
  var valueSerializer: KClass<out Serializer<*>> = StringSerializer::class

  private val scanPackages = mutableListOf<String>()
  private val defaultConsumerSettings = ConsumerSettings()
  private val defaultProducerSettings = ProducerSettings()
  private val additionalConsumerProps = mutableMapOf<String, Any>()
  private val additionalProducerProps = mutableMapOf<String, Any>()

  // Named factories
  private val consumerFactories = mutableMapOf<String, ConsumerFactoryConfig>()
  private val producerFactories = mutableMapOf<String, ProducerFactoryConfig>()

  // ─────────────────────────────────────────────────────────────────────────────
  // Consumer Packages
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Add packages to scan for @Component consumers with @KafkaListener methods.
   *
   * ```kotlin
   * consumerPackages("com.example.consumers", "com.example.handlers")
   * ```
   */
  fun consumerPackages(vararg packages: String) {
    scanPackages.addAll(packages)
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Default Consumer/Producer Settings
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Configure default consumer settings.
   *
   * ```kotlin
   * consumer {
   *   concurrency = 4
   *   pollTimeout = 1.seconds
   *   autoOffsetReset = "earliest"
   * }
   * ```
   */
  fun consumer(block: ConsumerSettings.() -> Unit) {
    defaultConsumerSettings.apply(block)
  }

  /**
   * Configure default producer settings.
   *
   * ```kotlin
   * producer {
   *   acks = "all"
   *   retries = 3
   *   compression = "lz4"
   * }
   * ```
   */
  fun producer(block: ProducerSettings.() -> Unit) {
    defaultProducerSettings.apply(block)
  }

  /**
   * Add additional default consumer properties.
   */
  fun consumerProperty(key: String, value: Any) {
    additionalConsumerProps[key] = value
  }

  /**
   * Add additional default producer properties.
   */
  fun producerProperty(key: String, value: Any) {
    additionalProducerProps[key] = value
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Named Consumer Factories (for multiple clusters/configurations)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Create a named consumer factory with custom configuration.
   *
   * The factory will be registered as `{name}ConsumerFactory` and the listener
   * container factory as `{name}KafkaListenerContainerFactory`.
   *
   * ```kotlin
   * consumerFactory("clusterB") {
   *   bootstrapServers = "cluster-b:9092"
   *   groupId = "my-app-cluster-b"
   *   concurrency = 2
   *   valueDeserializer = AvroDeserializer::class
   * }
   * ```
   *
   * Use in listener:
   * ```kotlin
   * @KafkaListener(
   *   topics = ["events"],
   *   containerFactory = "clusterBKafkaListenerContainerFactory"
   * )
   * ```
   *
   * @param name The factory name (used as prefix for bean names)
   * @param block Configuration block
   */
  fun consumerFactory(name: String, block: ConsumerFactoryConfig.() -> Unit) {
    require(name.isNotBlank()) { "Consumer factory name cannot be blank" }
    require(name != "default") { "Use 'consumer { }' block for default factory configuration" }

    val config = ConsumerFactoryConfig(name).apply {
      // Inherit defaults
      bootstrapServers = this@SpringKafkaConfig.bootstrapServers
      groupId = this@SpringKafkaConfig.groupId
      keyDeserializer = this@SpringKafkaConfig.keyDeserializer
      valueDeserializer = this@SpringKafkaConfig.valueDeserializer
    }
    config.apply(block)
    consumerFactories[name] = config
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Named Producer Factories (for multiple clusters/configurations)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Create a named producer factory with custom configuration.
   *
   * The factory will be registered as `{name}ProducerFactory` and the template
   * as `{name}KafkaTemplate`.
   *
   * ```kotlin
   * producerFactory("analytics") {
   *   bootstrapServers = "analytics-cluster:9092"
   *   acks = "1"
   *   compression = "snappy"
   * }
   * ```
   *
   * Access in code:
   * ```kotlin
   * val template = application.kafkaTemplate<String, Event>("analytics")
   * ```
   *
   * @param name The factory name (used as prefix for bean names)
   * @param block Configuration block
   */
  fun producerFactory(name: String, block: ProducerFactoryConfig.() -> Unit) {
    require(name.isNotBlank()) { "Producer factory name cannot be blank" }
    require(name != "default") { "Use 'producer { }' block for default factory configuration" }

    val config = ProducerFactoryConfig(name).apply {
      // Inherit defaults
      bootstrapServers = this@SpringKafkaConfig.bootstrapServers
      keySerializer = this@SpringKafkaConfig.keySerializer
      valueSerializer = this@SpringKafkaConfig.valueSerializer
    }
    config.apply(block)
    producerFactories[name] = config
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Internal Accessors
  // ─────────────────────────────────────────────────────────────────────────────

  internal fun getConsumerSettings() = defaultConsumerSettings

  internal fun getProducerSettings() = defaultProducerSettings

  internal fun getScanPackages() = scanPackages.toList()

  internal fun getAdditionalConsumerProps() = additionalConsumerProps.toMap()

  internal fun getAdditionalProducerProps() = additionalProducerProps.toMap()

  internal fun getConsumerFactories() = consumerFactories.toMap()

  internal fun getProducerFactories() = producerFactories.toMap()

  // ─────────────────────────────────────────────────────────────────────────────
  // Configuration Classes
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Default consumer settings (applied to default factory).
   */
  class ConsumerSettings {
    /**
     * Number of concurrent consumers.
     * Default: 1
     */
    var concurrency: Int = 1

    /**
     * Poll timeout for Kafka consumer.
     * Default: 1 second
     */
    var pollTimeout: Duration = 1.seconds

    /**
     * What to do when there is no initial offset.
     * Values: "earliest", "latest", "none"
     * Default: "earliest"
     */
    var autoOffsetReset: String = "earliest"

    /**
     * Whether to enable auto commit.
     * Default: false
     */
    var enableAutoCommit: Boolean = false

    /**
     * Maximum number of records returned in a single poll.
     * Default: 500
     */
    var maxPollRecords: Int = 500
  }

  /**
   * Default producer settings (applied to default factory).
   */
  class ProducerSettings {
    /**
     * Number of acknowledgments required.
     * Values: "0", "1", "all"
     * Default: "all"
     */
    var acks: String = "all"

    /**
     * Number of retries for failed sends.
     * Default: 3
     */
    var retries: Int = 3

    /**
     * Compression type.
     * Values: "none", "gzip", "snappy", "lz4", "zstd"
     * Default: "lz4"
     */
    var compression: String = "lz4"

    /**
     * Whether to enable idempotent producer.
     * Default: true
     */
    var idempotence: Boolean = true
  }

  /**
   * Configuration for a named consumer factory.
   *
   * Bean names generated:
   * - `{name}ConsumerFactory`
   * - `{name}KafkaListenerContainerFactory`
   */
  class ConsumerFactoryConfig(
    val name: String
  ) {
    /** Kafka bootstrap servers */
    var bootstrapServers: String = "localhost:9092"

    /** Consumer group ID */
    var groupId: String = "ktor-spring-kafka"

    /** Key deserializer class */
    var keyDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class

    /** Value deserializer class */
    var valueDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class

    /** Number of concurrent consumers */
    var concurrency: Int = 1

    /** Poll timeout */
    var pollTimeout: Duration = 1.seconds

    /** Auto offset reset policy */
    var autoOffsetReset: String = "earliest"

    /** Enable auto commit */
    var enableAutoCommit: Boolean = false

    /** Max poll records */
    var maxPollRecords: Int = 500

    /** Additional properties */
    private val additionalProps = mutableMapOf<String, Any>()

    /**
     * Add additional consumer property.
     */
    fun property(key: String, value: Any) {
      additionalProps[key] = value
    }

    internal fun getAdditionalProps() = additionalProps.toMap()
  }

  /**
   * Configuration for a named producer factory.
   *
   * Bean names generated:
   * - `{name}ProducerFactory`
   * - `{name}KafkaTemplate`
   */
  class ProducerFactoryConfig(
    val name: String
  ) {
    /** Kafka bootstrap servers */
    var bootstrapServers: String = "localhost:9092"

    /** Key serializer class */
    var keySerializer: KClass<out Serializer<*>> = StringSerializer::class

    /** Value serializer class */
    var valueSerializer: KClass<out Serializer<*>> = StringSerializer::class

    /** Number of acknowledgments required */
    var acks: String = "all"

    /** Number of retries */
    var retries: Int = 3

    /** Compression type */
    var compression: String = "lz4"

    /** Enable idempotent producer */
    var idempotence: Boolean = true

    /** Additional properties */
    private val additionalProps = mutableMapOf<String, Any>()

    /**
     * Add additional producer property.
     */
    fun property(key: String, value: Any) {
      additionalProps[key] = value
    }

    internal fun getAdditionalProps() = additionalProps.toMap()
  }
}
