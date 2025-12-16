package io.github.osoykan.kafkaflow.example

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.ExperimentalHoplite
import com.sksamuel.hoplite.addCommandLineSource
import com.sksamuel.hoplite.addResourceSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.example.api.configureRouting
import io.github.osoykan.kafkaflow.example.config.AppConfig
import io.github.osoykan.kafkaflow.example.infra.configureConsumerEngine
import io.github.osoykan.kafkaflow.example.infra.kafkaModule
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
import org.koin.ktor.plugin.Koin

private val logger = KotlinLogging.logger {}

/**
 * Main entry point for the Ktor Kafka Flow Example Application.
 *
 * This example demonstrates the lean consumer pattern with kafka-flow.
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

  logger.info { "Starting Ktor Kafka Flow Example on port ${config.server.port}" }

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
  // Install Koin DI
  install(Koin) {
    modules(
      module { single { config } },
      kafkaModule(config),
      applicationOverrides()
    )
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

  // Configure consumer engine lifecycle
  configureConsumerEngine()
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
