package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import org.apache.kafka.clients.consumer.ConsumerRecord

private val logger = KotlinLogging.logger {}

/**
 * Example event class for payment processing.
 */
data class PaymentEvent(
  val paymentId: String,
  val orderId: String,
  val amount: Double,
  val currency: String = "USD"
)

/**
 * Example payment consumer with manual acknowledgment.
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
  classifier = ClassifierType.ALWAYS_RETRY // Retry everything for payments
)
class PaymentConsumer : ConsumerManualAck<String, String> {
  /**
   * Process a payment with manual acknowledgment.
   *
   * You control exactly when to acknowledge - useful for ensuring
   * the payment is fully processed before committing the offset.
   */
  override suspend fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
    val paymentId = record.key()
    logger.info { "Processing payment: $paymentId" }

    try {
      // Simulate payment processing that must complete before ack
      processPayment(record.value())

      // Only acknowledge after payment is fully processed
      ack.acknowledge()
      logger.info { "Payment processed and acknowledged: $paymentId" }
    } catch (e: Exception) {
      logger.error(e) { "Payment processing failed: $paymentId" }
      // Don't acknowledge - let retry mechanism handle it
      throw e
    }
  }

  private fun processPayment(payload: String) {
    // Simulate payment processing
    if (payload.contains("insufficient-funds")) {
      throw RuntimeException("Insufficient funds - retry later")
    }

    // In real code: call payment gateway, update database, etc.
    Thread.sleep(100) // Simulate processing time
  }
}

/**
 * Consumer for notifications - demonstrates ALWAYS_RETRY classifier.
 *
 * For external API calls, you often want to retry all exceptions
 * because even validation-like errors might be transient on the remote side.
 */
@KafkaTopic(
  name = "example.notifications",
  retry = "example.notifications.retry",
  dlt = "example.notifications.dlt",
  concurrency = 4,
  maxInMemoryRetries = 3,
  maxRetryTopicAttempts = 5, // More attempts for notifications
  classifier = ClassifierType.ALWAYS_RETRY
)
class NotificationConsumer : ConsumerAutoAck<String, String> {
  override suspend fun consume(record: ConsumerRecord<String, String>) {
    logger.info { "Sending notification: ${record.key()}" }

    // Simulate external notification API call
    sendNotification(record.value())

    logger.info { "Notification sent: ${record.key()}" }
  }

  private fun sendNotification(payload: String) {
    // Simulate external API call that might fail transiently
    if (payload.contains("api-error")) {
      throw RuntimeException("External API temporarily unavailable")
    }
  }
}
