package io.github.osoykan.springkafka.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.springkafka.example.domain.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

/**
 * Example order consumer using Spring Kafka's @KafkaListener with Kotlin suspend function.
 *
 * Spring Kafka 3.2+ natively supports Kotlin suspend functions:
 * - The AckMode is automatically set to MANUAL for async handlers
 * - The message is acknowledged when the suspend function completes successfully
 * - If the handler throws, the message is not acknowledged and will be redelivered
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Async Returns</a>
 */
@Component
class OrderCreatedConsumer {
  /**
   * Process an order creation event.
   *
   * This is a suspend function - Spring Kafka handles it natively:
   * - Runs the handler in a coroutine context
   * - Acknowledges only after successful completion
   * - Properly propagates exceptions
   */
  @KafkaListener(topics = ["example.orders.created"])
  suspend fun consume(record: ConsumerRecord<String, DomainEvent>) {
    val event = record.value() as OrderCreatedEvent

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

  private suspend fun processOrder(event: OrderCreatedEvent) {
    // Simulate async processing
    logger.debug { "Order ${event.orderId} has ${event.items.size} items" }
    // In a real app, you could call async services here:
    // externalService.notifyOrderCreated(event)
  }
}
