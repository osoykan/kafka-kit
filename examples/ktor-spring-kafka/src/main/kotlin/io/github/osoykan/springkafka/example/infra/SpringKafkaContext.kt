package io.github.osoykan.springkafka.example.infra

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.springkafka.example.config.AppConfig
import io.github.osoykan.springkafka.example.domain.DomainEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties

private val logger = KotlinLogging.logger {}

/**
 * Minimal Spring ApplicationContext for Kafka integration.
 *
 * This allows using Spring Kafka's @KafkaListener annotation support
 * (including suspend functions) while using Ktor as the main framework
 * and Koin as the primary DI container.
 *
 * Spring Kafka 3.2+ supports Kotlin suspend functions natively:
 * - AckMode is automatically set to MANUAL for async handlers
 * - Acknowledgment happens when the suspend function completes
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Async Returns</a>
 */
class SpringKafkaContext(
  private val config: AppConfig
) {
  private lateinit var applicationContext: AnnotationConfigApplicationContext

  /**
   * Start the Spring Kafka context.
   * This initializes the ApplicationContext and starts all @KafkaListener containers.
   * Consumers are auto-discovered via @ComponentScan.
   */
  fun start() {
    logger.info { "Starting Spring Kafka context..." }

    applicationContext = AnnotationConfigApplicationContext().apply {
      // Register config as a bean so it can be injected
      beanFactory.registerSingleton("appConfig", config)

      // Register the configuration class which has @ComponentScan
      // This will auto-discover all @Component classes with @KafkaListener methods
      register(KafkaConfiguration::class.java)

      // Refresh to initialize all beans and start listeners
      refresh()
    }

    logger.info { "Spring Kafka context started" }
  }

  /**
   * Stop the Spring Kafka context.
   */
  fun stop() {
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
   * Check if the context is running.
   */
  fun isRunning(): Boolean = ::applicationContext.isInitialized && applicationContext.isRunning
}

/**
 * Spring Kafka configuration class.
 *
 * Uses @EnableKafka to enable @KafkaListener annotation processing,
 * which provides native support for Kotlin suspend functions.
 *
 * Uses @ComponentScan to auto-discover all @Component annotated consumers
 * in the consumers package.
 */
@Configuration
@EnableKafka
@ComponentScan(basePackages = ["io.github.osoykan.springkafka.example.consumers"])
open class KafkaConfiguration {
  @Bean
  open fun consumerFactory(config: AppConfig): ConsumerFactory<String, DomainEvent> =
    DefaultKafkaConsumerFactory(
      buildMap {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, config.kafka.groupId)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonDeserializer::class.java)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500)
        if (config.kafka.interceptorClasses.isNotBlank()) {
          put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(config.kafka.interceptorClasses))
        }
      }
    )

  @Bean
  open fun producerFactory(config: AppConfig): ProducerFactory<String, DomainEvent> =
    DefaultKafkaProducerFactory(
      buildMap {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, config.kafka.producer.acks)
        put(ProducerConfig.RETRIES_CONFIG, config.kafka.producer.retries)
        put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.kafka.producer.compression)
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        if (config.kafka.interceptorClasses.isNotBlank()) {
          put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(config.kafka.interceptorClasses))
        }
      }
    )

  @Bean
  open fun kafkaTemplate(producerFactory: ProducerFactory<String, DomainEvent>): KafkaTemplate<String, DomainEvent> =
    KafkaTemplate(producerFactory)

  /**
   * Listener container factory with support for suspend functions.
   *
   * When Spring Kafka detects a suspend function as a @KafkaListener handler:
   * - AckMode is automatically set to MANUAL
   * - The message is acknowledged when the suspend function completes
   * - Errors are handled properly (message not acknowledged on failure)
   */
  @Bean
  open fun kafkaListenerContainerFactory(
    consumerFactory: ConsumerFactory<String, DomainEvent>,
    config: AppConfig
  ): ConcurrentKafkaListenerContainerFactory<String, DomainEvent> =
    ConcurrentKafkaListenerContainerFactory<String, DomainEvent>().apply {
      setConsumerFactory(consumerFactory)
      setConcurrency(config.kafka.consumer.concurrency)
      containerProperties.pollTimeout = config.kafka.consumer.pollTimeout.inWholeMilliseconds
      // Note: For suspend functions, Spring Kafka automatically uses MANUAL ack mode
      // and handles acknowledgment when the coroutine completes
      containerProperties.ackMode = ContainerProperties.AckMode.RECORD
    }
}
