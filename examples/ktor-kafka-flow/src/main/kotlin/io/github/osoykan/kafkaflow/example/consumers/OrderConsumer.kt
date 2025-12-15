package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import org.apache.kafka.clients.consumer.ConsumerRecord

private val logger = KotlinLogging.logger {}

/**
 * Example event class for order creation.
 */
data class OrderCreatedEvent(
  val orderId: String,
  val customerId: String,
  val amount: Double,
  val items: List<String> = emptyList()
)

/**
 * Example order consumer demonstrating the lean consumer pattern.
 *
 * Notice how clean this is - just business logic!
 *
 * All the following are handled automatically by the supervisor:
 * - In-memory retries with exponential backoff
 * - Retry topic publishing when retries exhausted
 * - DLT publishing when retry topic attempts exhausted
 * - Metrics recording
 * - Exception classification
 * - TTL checks
 */
@KafkaTopic(
  name = "example.orders.created",
  retry = "example.orders.created.retry",
  dlt = "example.orders.created.dlt",
  concurrency = 2,
  maxInMemoryRetries = 3,
  maxRetryTopicAttempts = 2,
  backoffMs = 100,
  backoffMultiplier = 2.0,
  maxBackoffMs = 5_000,
  maxRetryDurationMs = 300_000, // 5 minutes max
  classifier = ClassifierType.DEFAULT // Validation errors → DLT immediately
)
class OrderCreatedConsumer : ConsumerAutoAck<String, String> {
  /**
   * Process an order creation event.
   *
   * This is ALL you write - no retry logic, no error handling boilerplate!
   */
  override suspend fun consume(record: ConsumerRecord<String, String>) {
    logger.info { "Processing order: ${record.key()} from ${record.topic()}" }

    // Simulate business logic
    val orderId = record.key()
    val payload = record.value()

    // Simulate potential failures for demo purposes
    if (payload.contains("fail-temporary")) {
      throw RuntimeException("Temporary failure - will be retried")
    }

    if (payload.contains("fail-validation")) {
      throw IllegalArgumentException("Invalid order data - will go to DLT immediately")
    }

    logger.info { "Successfully processed order: $orderId" }
  }
}

/**
 * Consumer for orders that failed and ended up in DLT.
 *
 * This is how you handle permanently failed messages - create a separate consumer!
 * You can alert, log, or take corrective action here.
 */
@KafkaTopic(
  name = "example.orders.created.dlt",
  maxInMemoryRetries = 0, // No retries for DLT consumer
  classifier = ClassifierType.NEVER_RETRY
)
class OrderCreatedDltConsumer : ConsumerAutoAck<String, String> {
  override suspend fun consume(record: ConsumerRecord<String, String>) {
    logger.error {
      """
            |Order permanently failed!
            |  Order ID: ${record.key()}
            |  Original topic: ${record.headerAsString("x-original-topic")}
            |  Exception: ${record.headerAsString("kafka.exception.class")}
            |  Message: ${record.headerAsString("kafka.exception.message")}
            |  Total retries: ${record.headerAsString("kafka.total.retry.count")}
      """.trimMargin()
    }

    // Here you could:
    // - Send an alert to your monitoring system
    // - Store in a database for manual review
    // - Send notification to support team
  }
}
