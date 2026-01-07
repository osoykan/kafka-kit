package io.github.osoykan.kafkaflow.example.e2e

import com.trendyol.stove.testing.e2e.*
import com.trendyol.stove.testing.e2e.database.migrations.DatabaseMigration
import com.trendyol.stove.testing.e2e.http.*
import com.trendyol.stove.testing.e2e.reporting.StoveKotestExtension
import com.trendyol.stove.testing.e2e.serialization.StoveSerde
import com.trendyol.stove.testing.e2e.standalone.kafka.*
import com.trendyol.stove.testing.e2e.system.*
import io.github.osoykan.kafkaflow.example.infra.objectMapper
import io.github.osoykan.kafkaflow.example.run
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import org.apache.kafka.clients.admin.NewTopic
import org.koin.core.module.Module
import org.koin.dsl.module

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
  override fun serialize(value: Any): ByteArray = when (value) {
    is String -> value.toByteArray()
    else -> objectMapper.writeValueAsBytes(value)
  }

  @Suppress("UNCHECKED_CAST")
  override fun <T : Any> deserialize(value: ByteArray, clazz: Class<T>): T {
    // First try to read with polymorphic type info (uses @class discriminator)
    val result = runCatching {
      objectMapper.readValue(value, Any::class.java)
    }.getOrElse {
      // Fallback: try direct deserialization to requested class
      objectMapper.readValue(value, clazz)
    }

    // If the result is assignable to the requested type, cast it
    return if (clazz.isInstance(result)) {
      result as T
    } else {
      // If types don't match, try direct deserialization
      objectMapper.readValue(value, clazz)
    }
  }
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
 * Stove E2E test setup for Ktor Kafka Flow example.
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

  override val extensions: List<Extension> = listOf(StoveKotestExtension())

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
          useEmbeddedKafka = true,
          listenPublishedMessagesFromStove = true,
          topicSuffixes = TopicSuffixes(
            error = listOf(".dlt"),
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
