package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.domain.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

/**
 * Example order consumer demonstrating the lean consumer pattern with strongly typed events.
 *
 * The consumer receives [OrderCreatedEvent] directly - no manual deserialization needed!
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
class OrderCreatedConsumer : ConsumerAutoAck<String, OrderCreatedEvent> {
  /**
   * Process an order creation event.
   *
   * This is ALL you write - no retry logic, no error handling boilerplate!
   */
  override suspend fun consume(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()

    logger.info {
      "Processing order: ${event.orderId} for customer ${event.customerId}, amount: ${event.amount} ${event.currency}"
    }

    // Validate the order
    validateOrder(event)

    // Process the order (in real app: save to DB, send notifications, etc.)
    processOrder(event)

    logger.info { "Successfully processed order: ${event.orderId}" }
  }

  private fun validateOrder(event: OrderCreatedEvent) {
    require(event.amount > BigDecimal.ZERO) {
      "Order amount must be positive: ${event.amount}"
    }
    require(event.items.isNotEmpty()) {
      "Order must have at least one item"
    }
  }

  private fun processOrder(event: OrderCreatedEvent) {
    // Simulate processing
    logger.debug { "Order ${event.orderId} has ${event.items.size} items" }
  }
}

/**
 * Consumer for orders that failed and ended up in DLT.
 *
 * DLT consumers also receive typed events - the original payload is preserved.
 */
@KafkaTopic(
  name = "example.orders.created.dlt",
  maxInMemoryRetries = 0, // No retries for DLT consumer
  classifier = ClassifierType.NEVER_RETRY
)
class OrderCreatedDltConsumer : ConsumerAutoAck<String, OrderCreatedEvent> {
  override suspend fun consume(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()

    logger.error {
      """
            |Order permanently failed!
            |  Order ID: ${event.orderId}
            |  Customer: ${event.customerId}
            |  Amount: ${event.amount}
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
