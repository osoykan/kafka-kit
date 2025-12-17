package io.github.osoykan.ktorkafka

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import io.ktor.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties

private val logger = KotlinLogging.logger {}

/**
 * Ktor plugin for Spring Kafka integration.
 *
 * Provides idiomatic Ktor configuration for Spring Kafka with:
 * - Native Kotlin suspend function support via @KafkaListener
 * - Auto-discovery of consumers via component scanning
 * - Multiple consumer/producer factories for different clusters
 * - External DI integration via [DependencyResolver]
 * - Lifecycle management tied to Ktor application
 *
 * ## Basic Usage
 *
 * ```kotlin
 * fun Application.module() {
 *   install(SpringKafka) {
 *     bootstrapServers = "localhost:9092"
 *     groupId = "my-consumer-group"
 *
 *     consumerPackages("com.example.consumers")
 *
 *     consumer {
 *       concurrency = 4
 *       pollTimeout = 1.seconds
 *     }
 *
 *     producer {
 *       acks = "all"
 *       retries = 3
 *     }
 *   }
 * }
 * ```
 *
 * ## With Koin Integration
 *
 * ```kotlin
 * // Define your Koin resolver
 * class KoinResolver(private val koin: Koin) : DependencyResolver {
 *   override fun <T : Any> resolve(type: KClass<T>): T? = koin.getOrNull(type)
 *   override fun <T : Any> resolve(type: KClass<T>, name: String): T? =
 *     koin.getOrNull(type, named(name))
 *   override fun canResolve(type: KClass<*>): Boolean = resolve(type) != null
 * }
 *
 * // Use it
 * install(Koin) { modules(appModule) }
 *
 * install(SpringKafka) {
 *   dependencyResolver = KoinResolver(getKoin())
 *   consumerPackages("com.example.consumers")
 * }
 * ```
 *
 * Now @KafkaListener consumers can inject Koin beans:
 *
 * ```kotlin
 * @Component
 * open class OrderConsumer(
 *   private val orderRepository: OrderRepository  // From Koin!
 * ) {
 *   @KafkaListener(topics = ["orders"])
 *   open suspend fun consume(record: ConsumerRecord<String, Order>) {
 *     orderRepository.save(record.value())
 *   }
 * }
 * ```
 *
 * @see DependencyResolver
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Spring Kafka Async Returns</a>
 */
val SpringKafka = createApplicationPlugin(name = "SpringKafka", createConfiguration = ::SpringKafkaConfig) {
  val config = pluginConfig

  // Use provided resolver or no-op
  val resolver = config.dependencyResolver ?: NoOpDependencyResolver

  // Create and start Spring context
  val springContext = SpringKafkaContextHolder(config, resolver)
  springContext.start()

  // Store in application attributes for access from routes
  application.attributes.put(SpringKafkaContextKey, springContext)

  // Stop on application shutdown
  application.monitor.subscribe(ApplicationStopPreparing) {
    logger.info { "Stopping Spring Kafka..." }
    springContext.stop()
  }
}

/**
 * Attribute key for accessing Spring Kafka context from application.
 */
val SpringKafkaContextKey = AttributeKey<SpringKafkaContextHolder>("SpringKafkaContext")

/**
 * Holds the Spring ApplicationContext for Kafka.
 *
 * Uses [FallbackBeanFactory] to delegate to external DI containers
 * when beans are not found in the Spring context.
 */
class SpringKafkaContextHolder internal constructor(
  private val config: SpringKafkaConfig,
  private val dependencyResolver: DependencyResolver
) {
  private lateinit var applicationContext: AnnotationConfigApplicationContext

  internal fun start() {
    logger.info { "Starting Spring Kafka context..." }

    // Create context with fallback bean factory
    val beanFactory = FallbackBeanFactory(dependencyResolver)
    applicationContext = AnnotationConfigApplicationContext(beanFactory)

    applicationContext.apply {
      // Register config as a bean
      beanFactory.registerSingleton("springKafkaConfig", config)

      // Create and register the dynamic configuration class
      register(DynamicKafkaConfiguration::class.java)

      // Register named factories
      registerNamedFactories(beanFactory, config)

      // Register component scan configuration if packages specified
      val packages = config.getScanPackages()
      if (packages.isNotEmpty()) {
        scan(*packages.toTypedArray())
        logger.info { "Scanning for @KafkaListener consumers in packages: $packages" }
      }

      refresh()
    }

    logger.info { "Spring Kafka context started" }
    logRegisteredFactories()

    if (dependencyResolver !is NoOpDependencyResolver) {
      logger.info { "External DI resolver configured: ${dependencyResolver::class.simpleName}" }
    }
  }

  private fun registerNamedFactories(beanFactory: FallbackBeanFactory, config: SpringKafkaConfig) {
    // Register named consumer factories
    config.getConsumerFactories().forEach { (_, factoryConfig) ->
      registerNamedConsumerFactory(beanFactory, factoryConfig)
    }

    // Register named producer factories
    config.getProducerFactories().forEach { (_, factoryConfig) ->
      registerNamedProducerFactory(beanFactory, factoryConfig)
    }
  }

  private fun registerNamedConsumerFactory(
    beanFactory: FallbackBeanFactory,
    config: SpringKafkaConfig.ConsumerFactoryConfig
  ) {
    val consumerFactoryName = "${config.name}ConsumerFactory"
    val containerFactoryName = "${config.name}KafkaListenerContainerFactory"

    // Create consumer factory
    val consumerProps = buildMap {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.keyDeserializer.java)
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.valueDeserializer.java)
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset)
      put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.enableAutoCommit)
      put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords)
      putAll(config.getAdditionalProps())
    }
    val consumerFactory = DefaultKafkaConsumerFactory<Any, Any>(consumerProps)
    beanFactory.registerSingleton(consumerFactoryName, consumerFactory)

    // Create listener container factory with virtual threads
    val containerFactory = ConcurrentKafkaListenerContainerFactory<Any, Any>().apply {
      setConsumerFactory(consumerFactory)
      setConcurrency(config.concurrency)
      containerProperties.pollTimeout = config.pollTimeout.inWholeMilliseconds
      containerProperties.ackMode = ContainerProperties.AckMode.RECORD
      containerProperties.listenerTaskExecutor = SimpleAsyncTaskExecutor().apply { setVirtualThreads(true) }
    }
    beanFactory.registerSingleton(containerFactoryName, containerFactory)

    logger.info { "Registered consumer factory '$consumerFactoryName' (virtual threads) for ${config.bootstrapServers}" }
  }

  private fun registerNamedProducerFactory(
    beanFactory: FallbackBeanFactory,
    config: SpringKafkaConfig.ProducerFactoryConfig
  ) {
    val producerFactoryName = "${config.name}ProducerFactory"
    val templateName = "${config.name}KafkaTemplate"

    // Create producer factory
    val producerProps = buildMap {
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.keySerializer.java)
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.valueSerializer.java)
      put(ProducerConfig.ACKS_CONFIG, config.acks)
      put(ProducerConfig.RETRIES_CONFIG, config.retries)
      put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compression)
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.idempotence)
      putAll(config.getAdditionalProps())
    }
    val producerFactory = DefaultKafkaProducerFactory<Any, Any>(producerProps)
    beanFactory.registerSingleton(producerFactoryName, producerFactory)

    // Create kafka template
    val template = KafkaTemplate(producerFactory)
    beanFactory.registerSingleton(templateName, template)

    logger.info { "Registered producer factory '$producerFactoryName' and template '$templateName' for ${config.bootstrapServers}" }
  }

  private fun logRegisteredFactories() {
    val consumerFactories = config.getConsumerFactories()
    val producerFactories = config.getProducerFactories()

    if (consumerFactories.isNotEmpty() || producerFactories.isNotEmpty()) {
      logger.info {
        buildString {
          appendLine("Registered Kafka factories:")
          appendLine("  Consumer factories:")
          appendLine("    - kafkaListenerContainerFactory (default)")
          consumerFactories.keys.forEach { name ->
            appendLine("    - ${name}KafkaListenerContainerFactory")
          }
          appendLine("  Producer factories:")
          appendLine("    - kafkaTemplate (default)")
          producerFactories.keys.forEach { name ->
            appendLine("    - ${name}KafkaTemplate")
          }
        }
      }
    }
  }

  internal fun stop() {
    if (::applicationContext.isInitialized) {
      logger.info { "Stopping Spring Kafka context..." }
      applicationContext.close()
      logger.info { "Spring Kafka context stopped" }
    }
  }

  /**
   * Get a bean from the Spring context.
   */
  fun <T : Any> getBean(clazz: Class<T>): T = applicationContext.getBean(clazz)

  /**
   * Get a named bean from the Spring context.
   */
  fun <T : Any> getBean(name: String, clazz: Class<T>): T = applicationContext.getBean(name, clazz)

  /**
   * Get a bean by name from the Spring context.
   */
  fun getBean(name: String): Any = applicationContext.getBean(name)

  /**
   * Check if a bean exists in the Spring context.
   */
  fun containsBean(name: String): Boolean = applicationContext.containsBean(name)

  /**
   * Check if the Spring context is running.
   */
  fun isRunning(): Boolean = ::applicationContext.isInitialized && applicationContext.isRunning
}

/**
 * Dynamic Kafka configuration that uses SpringKafkaConfig.
 * Creates default consumer factory, producer factory, and templates.
 */
@Configuration
@EnableKafka
@ComponentScan
internal open class DynamicKafkaConfiguration {
  @Bean
  open fun consumerFactory(config: SpringKafkaConfig): ConsumerFactory<Any, Any> =
    DefaultKafkaConsumerFactory(
      buildMap {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.keyDeserializer.java)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.valueDeserializer.java)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getConsumerSettings().autoOffsetReset)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getConsumerSettings().enableAutoCommit)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getConsumerSettings().maxPollRecords)
        putAll(config.getAdditionalConsumerProps())
      }
    )

  @Bean
  open fun producerFactory(config: SpringKafkaConfig): ProducerFactory<Any, Any> =
    DefaultKafkaProducerFactory(
      buildMap {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.keySerializer.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.valueSerializer.java)
        put(ProducerConfig.ACKS_CONFIG, config.getProducerSettings().acks)
        put(ProducerConfig.RETRIES_CONFIG, config.getProducerSettings().retries)
        put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProducerSettings().compression)
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.getProducerSettings().idempotence)
        putAll(config.getAdditionalProducerProps())
      }
    )

  @Bean
  open fun kafkaTemplate(producerFactory: ProducerFactory<Any, Any>): KafkaTemplate<Any, Any> =
    KafkaTemplate(producerFactory)

  @Bean
  open fun kafkaListenerContainerFactory(
    consumerFactory: ConsumerFactory<Any, Any>,
    config: SpringKafkaConfig
  ): ConcurrentKafkaListenerContainerFactory<Any, Any> =
    ConcurrentKafkaListenerContainerFactory<Any, Any>().apply {
      setConsumerFactory(consumerFactory)
      setConcurrency(config.getConsumerSettings().concurrency)
      containerProperties.pollTimeout = config.getConsumerSettings().pollTimeout.inWholeMilliseconds
      containerProperties.ackMode = ContainerProperties.AckMode.RECORD
      containerProperties.listenerTaskExecutor = SimpleAsyncTaskExecutor().apply { setVirtualThreads(true) }
    }
}
