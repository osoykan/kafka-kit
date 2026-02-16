package io.github.osoykan.kafkaflow.support

import org.springframework.kafka.test.EmbeddedKafkaKraftBroker

/**
 * EmbeddedKafka implementation for fast local TDD.
 *
 * Uses KRaft mode (no ZooKeeper) which is the modern Kafka deployment model.
 *
 * Benefits:
 * - Much faster startup (~2-3 seconds vs ~30 seconds for containers)
 * - No Docker dependency
 * - Perfect for rapid TDD cycles
 *
 * Trade-offs:
 * - Runs in-process (JVM memory)
 * - Not identical to production Kafka (but close enough for unit/integration tests)
 *
 * Usage:
 * ```kotlin
 * val kafka = EmbeddedKafkaBroker()
 * kafka.start()
 * // run tests
 * kafka.stop()
 * ```
 */
class EmbeddedKafkaBroker(
  private val partitions: Int = 3,
  private val brokerCount: Int = 1,
  private val topics: Array<String> = emptyArray()
) : AbstractKafkaBroker() {
  private var embeddedKafka: EmbeddedKafkaKraftBroker? = null

  override val bootstrapServers: String
    get() = embeddedKafka?.brokersAsString ?: throw IllegalStateException("EmbeddedKafka not started")

  override val isRunning: Boolean
    get() = embeddedKafka != null

  override fun start() {
    if (embeddedKafka != null) return

    embeddedKafka = EmbeddedKafkaKraftBroker(brokerCount, partitions, *topics).apply {
      afterPropertiesSet()
    }
    initAdminClient()
  }

  override fun stop() {
    closeAdminClient()
    embeddedKafka?.destroy()
    embeddedKafka = null
  }
}
