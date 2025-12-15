package io.github.osoykan.kafkaflow.example.infra

import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.config.AppConfig
import io.github.osoykan.kafkaflow.example.consumers.*
import io.ktor.server.application.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.koin.core.KoinApplication
import org.koin.core.module.Module
import org.koin.dsl.bind
import org.koin.dsl.module
import org.koin.ktor.ext.get
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

/**
 * Koin module for Kafka infrastructure.
 */
fun kafkaModule(config: AppConfig): Module = module {
  // Metrics - use NoOp for example, in production use MicrometerMetrics
  single<KafkaFlowMetrics> { LoggingMetrics() }

  // Consumer factory
  single {
    val props = mapOf(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.kafka.bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG to config.kafka.groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 500
    )
    DefaultKafkaConsumerFactory<String, String>(props)
  }

  // Producer factory & template
  single {
    val props = mapOf(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.kafka.bootstrapServers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
      ProducerConfig.ACKS_CONFIG to config.kafka.producer.acks,
      ProducerConfig.RETRIES_CONFIG to config.kafka.producer.retries,
      ProducerConfig.COMPRESSION_TYPE_CONFIG to config.kafka.producer.compression,
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true
    )
    DefaultKafkaProducerFactory<String, String>(props)
  }

  single {
    KafkaTemplate(get<DefaultKafkaProducerFactory<String, String>>())
  }

  // Listener config
  single {
    ListenerConfig(
      concurrency = config.kafka.consumer.concurrency,
      pollTimeout = config.kafka.consumer.pollTimeout,
      useVirtualThreads = config.kafka.useVirtualThreads
    )
  }

  // Topic resolver
  single {
    TopicResolver(
      defaultGroupId = config.kafka.groupId,
      defaultRetryPolicy = RetryPolicy.DEFAULT
    )
  }

  // Supervisor factory
  single<ConsumerSupervisorFactory<String, String>> {
    DefaultConsumerSupervisorFactory(
      consumerFactory = get(),
      kafkaTemplate = get(),
      topicResolver = get(),
      listenerConfig = get(),
      metrics = get()
    )
  }

  // Consumer engine - discovers all consumers
  single {
    ConsumerEngine(
      consumers = getAll<Consumer<String, String>>(),
      supervisorFactory = get(),
      metrics = get(),
      enabled = config.kafka.consumer.enabled
    )
  }

  // ─────────────────────────────────────────────────────────────
  // Register your consumers here!
  // They will be auto-discovered by ConsumerEngine
  // ─────────────────────────────────────────────────────────────

  single { OrderCreatedConsumer() } bind Consumer::class
  single { OrderCreatedDltConsumer() } bind Consumer::class
  single { PaymentConsumer() } bind Consumer::class
  single { NotificationConsumer() } bind Consumer::class
}

/**
 * Extension to register Kafka with Koin.
 */
fun KoinApplication.registerKafka(config: AppConfig) {
  modules(kafkaModule(config))
}

/**
 * Configure consumer engine lifecycle with Ktor application.
 */
fun Application.configureConsumerEngine() {
  val consumerEngine = get<ConsumerEngine<String, String>>()

  monitor.subscribe(ApplicationStarted) {
    consumerEngine.start()
  }

  monitor.subscribe(ApplicationStopPreparing) {
    consumerEngine.stop()
  }
}
