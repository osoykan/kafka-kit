package io.github.osoykan.springkafka.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.springkafka.example.domain.*
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

/**
 * Example payment consumer using Spring Kafka's @KafkaListener with Kotlin suspend function.
 *
 * **Key Feature Demonstration:**
 * This consumer injects [NotificationService] which is managed by **Koin**,
 * showing how the dependency bridge works for multiple consumers.
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Async Returns</a>
 */
@Component
class PaymentConsumer(
  // NotificationService is managed by Koin and automatically injected via DependencyResolver!
  private val notificationService: NotificationService
) {
  /**
   * Process a payment event.
   *
   * This suspend function can perform async operations like calling external APIs.
   * Spring Kafka will acknowledge the message only after successful processing.
   */
  @KafkaListener(topics = ["example.payments"])
  suspend fun consume(record: ConsumerRecord<String, DomainEvent>) {
    val event = record.value() as PaymentEvent

    logger.info {
      "Processing payment: ${event.paymentId} for order ${event.orderId}, amount: ${event.amount} ${event.currency}"
    }

    // Process payment - can call async payment gateway here
    processPayment(event)

    // Send payment confirmation (Koin-managed service!)
    notificationService.sendPaymentConfirmation(event)

    logger.info { "Payment processed successfully: ${event.paymentId}" }
  }

  private suspend fun processPayment(event: PaymentEvent) {
    // Validate payment
    require(event.amount > BigDecimal.ZERO) {
      "Payment amount must be positive"
    }

    // In real code: call payment gateway, update database, etc.
    logger.debug { "Processing ${event.method} payment of ${event.amount} ${event.currency}" }

    // Simulate async processing time (e.g., calling payment gateway)
    delay(50)
  }
}
