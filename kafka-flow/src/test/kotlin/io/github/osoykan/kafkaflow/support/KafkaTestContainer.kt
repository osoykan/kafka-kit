package io.github.osoykan.kafkaflow.support

/**
 * Kafka test container wrapper for testing.
 *
 * @deprecated Use [SharedKafka.instance] instead for automatic strategy selection,
 * or [TestcontainersKafkaBroker] / [EmbeddedKafkaBroker] directly.
 */
@Deprecated(
  message = "Use SharedKafka.instance for automatic strategy selection",
  replaceWith = ReplaceWith("SharedKafka.instance", "io.github.osoykan.kafkaflow.support.SharedKafka")
)
typealias KafkaTestContainer = TestcontainersKafkaBroker

/**
 * Test container extension for Kotest.
 *
 * @deprecated Use [SharedKafka.instance] instead for automatic strategy selection.
 */
@Deprecated(
  message = "Use SharedKafka.instance instead",
  replaceWith = ReplaceWith("SharedKafka.instance", "io.github.osoykan.kafkaflow.support.SharedKafka")
)
fun withKafka(
  imageTag: String = "7.8.0",
  block: suspend TestcontainersKafkaBroker.() -> Unit
) {
  val kafka = TestcontainersKafkaBroker(imageTag)
  try {
    kafka.start()
    kotlinx.coroutines.runBlocking {
      kafka.block()
    }
  } finally {
    kafka.stop()
  }
}
