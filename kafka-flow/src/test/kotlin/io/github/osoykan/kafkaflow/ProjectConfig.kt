package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.kotest.core.config.AbstractProjectConfig

/**
 * Kotest project configuration.
 * Starts a single shared Kafka container before all tests and stops it after.
 */
class ProjectConfig : AbstractProjectConfig() {
  override suspend fun beforeProject() {
    SharedKafka.start()
  }

  override suspend fun afterProject() {
    SharedKafka.stop()
  }
}
