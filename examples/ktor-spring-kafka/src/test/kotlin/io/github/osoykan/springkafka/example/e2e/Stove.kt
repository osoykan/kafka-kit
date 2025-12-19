package io.github.osoykan.springkafka.example.e2e

import com.trendyol.stove.testing.e2e.*
import com.trendyol.stove.testing.e2e.database.migrations.DatabaseMigration
import com.trendyol.stove.testing.e2e.http.*
import com.trendyol.stove.testing.e2e.serialization.StoveSerde
import com.trendyol.stove.testing.e2e.standalone.kafka.*
import com.trendyol.stove.testing.e2e.system.*
import io.github.osoykan.springkafka.example.run
import io.kotest.core.config.AbstractProjectConfig
import org.apache.kafka.clients.admin.NewTopic
import org.koin.core.module.Module
import org.koin.dsl.module
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.cfg.DateTimeFeature
import tools.jackson.module.kotlin.*

/**
 * Custom StoveSerde that aligns with the app's Jackson serialization.
 *
 * Uses the same objectMapper as the application to ensure consistent
 * serialization/deserialization of domain events.
 *
 * Handles @JsonTypeInfo polymorphic serialization by first deserializing
 * to Any (letting Jackson use the @class discriminator), then checking
 * if the result matches the requested type.
 */
class AppAlignedSerde : StoveSerde<Any, ByteArray> {
  private val serde = jsonMapper {
    addModule(kotlinModule())
    findAndAddModules()
    disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    disable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)
    disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
    disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    enable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
  }

  override fun serialize(value: Any): ByteArray = when (value) {
    is String -> value.toByteArray()
    else -> serde.writeValueAsBytes(value)
  }

  override fun <T : Any> deserialize(value: ByteArray, clazz: Class<T>): T = runCatching {
    serde.readValue(value, clazz) as T
  }.getOrElse { error("Unable to deserialize value $value of ${clazz.simpleName}") }
}

/**
 * Migration to create all required Kafka topics for the example application.
 */
class CreateExampleTopicsMigration : DatabaseMigration<KafkaMigrationContext> {
  override val order: Int = 1

  override suspend fun execute(connection: KafkaMigrationContext) {
    val topics = listOf(
      // Order topics
      NewTopic("example.orders.created", 1, 1),
      NewTopic("example.orders.created.retry", 1, 1),
      NewTopic("example.orders.created.dlt", 1, 1),
      // Payment topics
      NewTopic("example.payments", 1, 1),
      NewTopic("example.payments.retry", 1, 1),
      NewTopic("example.payments.dlt", 1, 1),
      // Notification topics
      NewTopic("example.notifications", 1, 1),
      NewTopic("example.notifications.retry", 1, 1),
      NewTopic("example.notifications.dlt", 1, 1)
    )

    connection.admin
      .createTopics(topics)
      .all()
      .get()
  }
}

/**
 * Stove E2E test setup for Ktor Spring Kafka example.
 *
 * Uses embedded Kafka for fast, isolated testing.
 */
class Stove : AbstractProjectConfig() {
  companion object {
    init {
      stoveKafkaBridgePortDefault = PortFinder.findAvailablePortAsString()
      System.setProperty(STOVE_KAFKA_BRIDGE_PORT, stoveKafkaBridgePortDefault)
    }
  }

  override suspend fun beforeProject(): Unit = TestSystem()
    .with {
      httpClient {
        HttpClientSystemOptions(
          baseUrl = "http://localhost:8080"
        )
      }
      bridge()
      kafka {
        KafkaSystemOptions(
          serde = AppAlignedSerde(),
          valueSerializer = JacksonJsonSerializer(),
          useEmbeddedKafka = true,
          listenPublishedMessagesFromStove = true,
          topicSuffixes = TopicSuffixes(
            error = listOf(".dlt", ".DLT"),
            retry = listOf(".retry")
          ),
          configureExposedConfiguration = { cfg ->
            listOf(
              "kafka.bootstrap-servers=${cfg.bootstrapServers}",
              "kafka.interceptor-classes=${cfg.interceptorClass}"
            )
          }
        ).migrations {
          register<CreateExampleTopicsMigration>()
        }
      }
      ktor(
        withParameters = listOf(
          "port=8080"
        ),
        runner = { parameters ->
          run(parameters) {
            addTestSystemDependencies()
          }
        }
      )
    }.run()

  override suspend fun afterProject() {
    TestSystem.stop()
  }
}

/**
 * Test system dependencies override module.
 * Add any test-specific beans or mocks here.
 */
fun addTestSystemDependencies(): Module = module {
  // Add test-specific overrides here if needed
  // Example: single { MockService() }.bind<ServiceInterface>()
}
