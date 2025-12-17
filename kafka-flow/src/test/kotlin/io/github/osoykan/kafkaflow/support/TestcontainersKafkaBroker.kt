package io.github.osoykan.kafkaflow.support

import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Testcontainers implementation for CI/production-like testing.
 *
 * Benefits:
 * - Real Kafka in Docker container
 * - Production-identical behavior
 * - Network isolation
 *
 * Trade-offs:
 * - Slower startup (~30 seconds)
 * - Requires Docker
 *
 * Best for:
 * - CI pipelines
 * - Integration tests that need production-like behavior
 * - Testing with specific Kafka versions
 *
 * Usage:
 * ```kotlin
 * val kafka = TestcontainersKafkaBroker()
 * kafka.start()
 * // run tests
 * kafka.stop()
 * ```
 */
class TestcontainersKafkaBroker(
  private val imageTag: String = "7.8.0"
) : AbstractKafkaBroker() {
  private val container = ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:$imageTag")
  )

  override val bootstrapServers: String
    get() = container.bootstrapServers

  override val isRunning: Boolean
    get() = container.isRunning

  override fun start() {
    if (container.isRunning) return
    container.start()
    initAdminClient()
  }

  override fun stop() {
    closeAdminClient()
    container.stop()
  }
}
