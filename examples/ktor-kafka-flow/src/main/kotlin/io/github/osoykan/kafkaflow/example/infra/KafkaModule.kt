package io.github.osoykan.kafkaflow.example.infra

import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.config.AppConfig
import io.github.osoykan.kafkaflow.example.consumers.*
import io.github.osoykan.kafkaflow.example.domain.DomainEvent
import io.ktor.server.application.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.koin.core.module.Module
import org.koin.dsl.bind
import org.koin.dsl.module
import org.koin.ktor.ext.get

/**
 * Koin module for Kafka infrastructure.
 *
 * Uses KafkaFlowFactory to abstract away the underlying Kafka client implementation.
 * Messages are serialized/deserialized using Jackson for strongly typed domain events.
 */
fun kafkaModule(config: AppConfig): Module = module {
  // Metrics - use NoOp for example, in production use MicrometerMetrics
  single<KafkaFlowMetrics> { LoggingMetrics() }

  // KafkaFlow factory - abstracts Spring Kafka internals
  // Uses String keys and DomainEvent values with Jackson serde
  single {
    KafkaFlowFactory.create<String, DomainEvent>(
      KafkaFlowFactoryConfig(
        consumerProperties = consumerProperties(config),
        producerProperties = producerProperties(config),
        listenerConfig = ListenerConfig(
          concurrency = config.kafka.consumer.concurrency,
          pollTimeout = config.kafka.consumer.pollTimeout
        ),
        metrics = get(),
        topicResolver = TopicResolver(
          defaultRetryPolicy = RetryPolicy.DEFAULT
        )
      )
    )
  }

  // Consumer engine - discovers all consumers
  single {
    get<KafkaFlowFactory<String, DomainEvent>>().createConsumerEngine(
      consumers = getAll<Consumer<String, DomainEvent>>(),
      enabled = config.kafka.consumer.enabled
    )
  }

  // Register your consumers here - they will be auto-discovered by ConsumerEngine
  single { OrderCreatedConsumer() } bind Consumer::class
  single { OrderCreatedDltConsumer() } bind Consumer::class
  single { PaymentConsumer() } bind Consumer::class
  single { NotificationConsumer() } bind Consumer::class
}

/**
 * Build consumer properties map with Jackson deserializer for domain events.
 */
private fun consumerProperties(config: AppConfig): Map<String, Any> = buildMap {
  put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
  put(ConsumerConfig.GROUP_ID_CONFIG, config.kafka.groupId)
  put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
  put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonDeserializer::class.java)
  put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
  put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500)
  // Add interceptors if configured (used by Stove for message observation)
  if (config.kafka.interceptorClasses.isNotBlank()) {
    put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(config.kafka.interceptorClasses))
  }
}

/**
 * Build producer properties map with Jackson serializer for domain events.
 */
private fun producerProperties(config: AppConfig): Map<String, Any> = buildMap {
  put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
  put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
  put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonSerializer::class.java)
  put(ProducerConfig.ACKS_CONFIG, config.kafka.producer.acks)
  put(ProducerConfig.RETRIES_CONFIG, config.kafka.producer.retries)
  put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.kafka.producer.compression)
  put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
  // Add interceptors if configured (used by Stove for message observation)
  if (config.kafka.interceptorClasses.isNotBlank()) {
    put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(config.kafka.interceptorClasses))
  }
}

/**
 * Configure consumer engine lifecycle with Ktor application.
 */
fun Application.configureConsumerEngine() {
  val consumerEngine = get<ConsumerEngine<String, DomainEvent>>()

  monitor.subscribe(ApplicationStarted) {
    consumerEngine.start()
  }

  monitor.subscribe(ApplicationStopPreparing) {
    consumerEngine.stop()
  }
}
