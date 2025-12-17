package io.github.osoykan.kafkaflow.support

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Shared Kafka broker instance for all tests.
 *
 * Strategy selection:
 * - Local TDD (default): Uses EmbeddedKafka for fast feedback (~2-3s startup)
 * - CI: Uses Testcontainers for production-like testing (~30s startup)
 *
 * Environment variable control:
 * - KAFKA_TEST_MODE=embedded (default) → EmbeddedKafka
 * - KAFKA_TEST_MODE=testcontainers → Testcontainers
 * - CI=true or GITHUB_ACTIONS=true → Testcontainers
 *
 * Usage in tests:
 * ```kotlin
 * class MyTests : FunSpec({
 *     val kafka = SharedKafka.instance
 *
 *     test("my test") {
 *         kafka.createTopic("my-topic")
 *         // ...
 *     }
 * })
 * ```
 */
object SharedKafka {
  private val mode: KafkaTestMode by lazy { detectMode() }
  private var broker: AbstractKafkaBroker? = null
  private var shutdownHookRegistered = false

  /**
   * Gets the shared Kafka broker instance, starting it if necessary.
   */
  val instance: AbstractKafkaBroker
    get() {
      start()
      return broker!!
    }

  /**
   * Explicitly starts the shared Kafka broker.
   */
  fun start() {
    if (broker != null) return

    broker = createBroker()
    broker!!.start()
    println("╔══════════════════════════════════════════════════════════════════╗")
    println("║ Kafka Test Mode: $mode")
    println("║ Bootstrap: ${broker!!.bootstrapServers}")
    println("╚══════════════════════════════════════════════════════════════════╝")
    logger.info { "Started Kafka broker (mode=$mode): ${broker!!.bootstrapServers}" }

    // Register shutdown hook (only once)
    if (!shutdownHookRegistered) {
      Runtime.getRuntime().addShutdownHook(
        Thread {
          stop()
        }
      )
      shutdownHookRegistered = true
    }
  }

  /**
   * Explicitly stops the shared Kafka broker.
   */
  fun stop() {
    broker?.stop()
    broker = null
    logger.info { "Stopped Kafka broker" }
  }

  /**
   * Forces a specific mode (useful for testing the test infrastructure itself).
   */
  fun withMode(mode: KafkaTestMode, block: (AbstractKafkaBroker) -> Unit) {
    val broker = when (mode) {
      KafkaTestMode.EMBEDDED -> EmbeddedKafkaBroker()
      KafkaTestMode.TESTCONTAINERS -> TestcontainersKafkaBroker()
    }
    try {
      broker.start()
      block(broker)
    } finally {
      broker.stop()
    }
  }

  private fun createBroker(): AbstractKafkaBroker = when (mode) {
    KafkaTestMode.EMBEDDED -> EmbeddedKafkaBroker()
    KafkaTestMode.TESTCONTAINERS -> TestcontainersKafkaBroker()
  }

  private fun detectMode(): KafkaTestMode {
    // Check explicit mode setting
    val explicitMode = System.getenv("KAFKA_TEST_MODE")?.uppercase()
    if (explicitMode == "TESTCONTAINERS") {
      logger.info { "Using Testcontainers (KAFKA_TEST_MODE=testcontainers)" }
      return KafkaTestMode.TESTCONTAINERS
    }
    if (explicitMode == "EMBEDDED") {
      logger.info { "Using EmbeddedKafka (KAFKA_TEST_MODE=embedded)" }
      return KafkaTestMode.EMBEDDED
    }

    // Auto-detect CI environment
    val isCI = System.getenv("CI")?.toBoolean() == true ||
      System.getenv("GITHUB_ACTIONS")?.toBoolean() == true ||
      System.getenv("GITLAB_CI")?.toBoolean() == true ||
      System.getenv("JENKINS_URL") != null

    return if (isCI) {
      logger.info { "CI environment detected, using Testcontainers" }
      KafkaTestMode.TESTCONTAINERS
    } else {
      logger.info { "Local environment detected, using EmbeddedKafka for fast TDD" }
      KafkaTestMode.EMBEDDED
    }
  }
}

/**
 * Kafka test mode selection.
 */
enum class KafkaTestMode {
  /**
   * EmbeddedKafka - Fast startup (~2-3s), runs in-process.
   * Best for: Local TDD, unit tests, fast feedback loops.
   */
  EMBEDDED,

  /**
   * Testcontainers - Real Kafka in Docker (~30s startup).
   * Best for: CI, integration tests, production-like behavior.
   */
  TESTCONTAINERS
}
