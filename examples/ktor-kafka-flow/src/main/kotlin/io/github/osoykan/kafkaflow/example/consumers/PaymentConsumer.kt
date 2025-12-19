package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.domain.PaymentEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

/**
 * Example payment consumer with manual acknowledgment and strongly typed events.
 *
 * The consumer receives [PaymentEvent] directly - no manual deserialization needed!
 *
 * Use manual ack when you need explicit control over when offsets are committed,
 * such as when:
 * - Processing involves external side effects that must complete first
 * - You need to batch operations before committing
 * - You want exactly-once semantics with external systems
 */
@KafkaTopic(
  name = "example.payments",
  retry = "example.payments.retry",
  dlt = "example.payments.dlt",
  concurrency = 1, // Sequential processing for payments
  maxInMemoryRetries = 5, // More retries for important payments
  maxRetryTopicAttempts = 3,
  backoffMs = 200,
  backoffMultiplier = 2.0,
  maxBackoffMs = 10_000,
  commitSize = 1,
  commitIntervalMs = 100,
  classifier = ClassifierType.ALWAYS_RETRY // Retry everything for payments
)
class PaymentConsumer : ConsumerManualAck<String, PaymentEvent> {
  /**
   * Process a payment with manual acknowledgment.
   *
   * You control exactly when to acknowledge - useful for ensuring
   * the payment is fully processed before committing the offset.
   */
  override suspend fun consume(record: ConsumerRecord<String, PaymentEvent>, ack: Acknowledgment) {
    val event = record.value()

    logger.info {
      "Processing payment: ${event.paymentId} for order ${event.orderId}, amount: ${event.amount} ${event.currency}"
    }

    try {
      // Process payment - must complete before ack
      processPayment(event)

      // Only acknowledge after payment is fully processed
      ack.acknowledge()
      logger.info { "Payment processed and acknowledged: ${event.paymentId}" }
    } catch (e: Exception) {
      logger.error(e) { "Payment processing failed: ${event.paymentId}" }
      // Don't acknowledge - let retry mechanism handle it
      throw e
    }
  }

  private fun processPayment(event: PaymentEvent) {
    // Validate payment
    require(event.amount > BigDecimal.ZERO) {
      "Payment amount must be positive"
    }

    // In real code: call payment gateway, update database, etc.
    logger.debug { "Processing ${event.method} payment of ${event.amount} ${event.currency}" }
    Thread.sleep(50) // Simulate processing time
  }
}
