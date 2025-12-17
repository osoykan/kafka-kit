package io.github.osoykan.springkafka.example.config

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Main application configuration.
 * Loaded from application.yaml using Hoplite.
 */
data class AppConfig(
  val server: ServerConfig = ServerConfig(),
  val kafka: KafkaConfig = KafkaConfig()
)

data class ServerConfig(
  val port: Int = 8080,
  val host: String = "0.0.0.0"
)

data class KafkaConfig(
  val bootstrapServers: String = "localhost:9092",
  val groupId: String = "ktor-spring-kafka-example",
  val interceptorClasses: String = "",
  val consumer: ConsumerSettings = ConsumerSettings(),
  val producer: ProducerSettings = ProducerSettings()
)

data class ConsumerSettings(
  val enabled: Boolean = true,
  val concurrency: Int = 4,
  val pollTimeout: Duration = 1.seconds
)

data class ProducerSettings(
  val acks: String = "all",
  val retries: Int = 3,
  val compression: String = "lz4"
)
