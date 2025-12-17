package io.github.osoykan.springkafka.example

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.ExperimentalHoplite
import com.sksamuel.hoplite.addCommandLineSource
import com.sksamuel.hoplite.addResourceSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.ktorkafka.SpringKafka
import io.github.osoykan.springkafka.example.api.configureRouting
import io.github.osoykan.springkafka.example.config.AppConfig
import io.github.osoykan.springkafka.example.domain.NotificationService
import io.github.osoykan.springkafka.example.domain.OrderRepository
import io.github.osoykan.springkafka.example.infra.JacksonDeserializer
import io.github.osoykan.springkafka.example.infra.JacksonSerializer
import io.github.osoykan.springkafka.example.infra.KoinDependencyResolver
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.autohead.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import org.koin.core.module.Module
import org.koin.dsl.module
import org.koin.ktor.ext.getKoin
import org.koin.ktor.plugin.Koin

private val logger = KotlinLogging.logger {}

/**
 * Main entry point for the Ktor Spring Kafka Example Application.
 *
 * This example demonstrates how to use Spring Kafka directly with Ktor,
 * taking advantage of Kotlin suspend function support introduced in Spring Kafka 3.2+.
 *
 * Key features:
 * - Ktor as the web framework and application container
 * - Koin as the DI container (optional, for non-Kafka beans)
 * - Spring Kafka plugin for Kafka consumer/producer functionality
 * - Kotlin suspend functions as message handlers
 * - Auto-discovery of @KafkaListener consumers via component scanning
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Spring Kafka Async Returns</a>
 */
fun main(args: Array<String>) {
  run(args, shouldWait = true)
}

/**
 * Run the application with optional test overrides.
 * This function is used both for normal startup and for testing with Stove.
 */
fun run(
  args: Array<String>,
  shouldWait: Boolean = false,
  applicationOverrides: () -> Module = { module { } }
): Application {
  val config = loadConfig(args)

  logger.info { "Starting Ktor Spring Kafka Example on port ${config.server.port}" }

  val applicationEngine = embeddedServer(
    Netty,
    port = config.server.port,
    host = config.server.host
  ) {
    appModule(config, applicationOverrides)
  }

  applicationEngine.start(wait = shouldWait)
  return applicationEngine.application
}

/**
 * Configure the Ktor application module.
 */
fun Application.appModule(
  config: AppConfig,
  applicationOverrides: () -> Module = { module { } }
) {
  // Install Koin FIRST - Spring Kafka consumers will use Koin-managed services
  install(Koin) {
    modules(
      module {
        single { config }

        // Domain services - these will be injected into Spring Kafka consumers via DependencyResolver!
        // Spring beans (EventMetricsService, EventValidator) are discovered separately by Spring
        single { OrderRepository() }
        single { NotificationService() }
      },
      applicationOverrides()
    )
  }

  // Install Spring Kafka plugin - with dependency bridge to Koin!
  install(SpringKafka) {
    bootstrapServers = config.kafka.bootstrapServers
    groupId = config.kafka.groupId

    // Bridge: Allow Spring Kafka consumers to inject Koin-managed beans
    dependencyResolver = KoinDependencyResolver(getKoin())

    // Custom serializers/deserializers for domain events
    valueSerializer = JacksonSerializer::class
    valueDeserializer = JacksonDeserializer::class

    // Auto-discover consumers and Spring services
    consumerPackages(
      "io.github.osoykan.springkafka.example.consumers",
      "io.github.osoykan.springkafka.example.infra"
    )

    consumer {
      concurrency = config.kafka.consumer.concurrency
      pollTimeout = config.kafka.consumer.pollTimeout
    }

    producer {
      acks = config.kafka.producer.acks
      retries = config.kafka.producer.retries
      compression = config.kafka.producer.compression
    }

    // Add interceptors for Stove testing if configured
    if (config.kafka.interceptorClasses.isNotBlank()) {
      consumerProperty("interceptor.classes", listOf(config.kafka.interceptorClasses))
      producerProperty("interceptor.classes", listOf(config.kafka.interceptorClasses))
    }
  }

  // Install plugins
  install(AutoHeadResponse)
  install(ContentNegotiation) {
    jackson {
      registerModule(JavaTimeModule())
      disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }
  }
  install(StatusPages) {
    exception<Throwable> { call, cause ->
      logger.error(cause) { "Unhandled exception" }
      call.respond(
        HttpStatusCode.InternalServerError,
        mapOf("error" to (cause.message ?: "Unknown error"))
      )
    }
  }

  // Configure routes
  configureRouting()
}

/**
 * Load configuration from application.yaml using Hoplite.
 * Command-line args override file config (first source wins in Hoplite).
 */
@OptIn(ExperimentalHoplite::class)
fun loadConfig(args: Array<String> = emptyArray()): AppConfig = ConfigLoaderBuilder
  .default()
  .addCommandLineSource(args) // First - so it overrides file config
  .addResourceSource("/application.yaml")
  .withExplicitSealedTypes()
  .build()
  .loadConfigOrThrow<AppConfig>()
