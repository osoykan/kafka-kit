package io.github.osoykan.springkafka.example.infra

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.springkafka.example.config.AppConfig
import io.github.osoykan.springkafka.example.domain.DomainEvent
import io.ktor.server.application.*
import org.koin.core.module.Module
import org.koin.dsl.module
import org.koin.ktor.ext.get
import org.springframework.kafka.core.KafkaTemplate

private val logger = KotlinLogging.logger {}

/**
 * Koin module for Kafka infrastructure using Spring Kafka with @KafkaListener.
 *
 * This module creates a minimal Spring ApplicationContext for Kafka that:
 * - Enables @KafkaListener annotation processing
 * - Supports Kotlin suspend functions natively (Spring Kafka 3.2+)
 * - Integrates with Ktor lifecycle
 *
 * The Spring context handles:
 * - Consumer factory and producer factory creation
 * - KafkaTemplate for producing messages
 * - @KafkaListener discovery and container management
 * - Automatic acknowledgment for suspend function handlers
 */
fun kafkaModule(config: AppConfig): Module = module {
  // Spring Kafka context - manages all Kafka beans and listeners
  single {
    SpringKafkaContext(config)
  }

  // KafkaTemplate - obtained from Spring context for producing messages
  @Suppress("UNCHECKED_CAST")
  single<KafkaTemplate<String, DomainEvent>> {
    get<SpringKafkaContext>().getBean(KafkaTemplate::class.java) as KafkaTemplate<String, DomainEvent>
  }
}

/**
 * Configure Spring Kafka lifecycle with Ktor application.
 *
 * Starts the Spring Kafka context when Ktor starts,
 * and stops it when Ktor stops.
 */
fun Application.configureKafkaConsumers() {
  val springKafkaContext = get<SpringKafkaContext>()

  monitor.subscribe(ApplicationStarted) {
    logger.info { "Starting Kafka consumers..." }
    springKafkaContext.start()
  }

  monitor.subscribe(ApplicationStopPreparing) {
    logger.info { "Stopping Kafka consumers..." }
    springKafkaContext.stop()
  }
}
