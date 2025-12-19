package io.github.osoykan.springkafka.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafka.toolkit.dumpThreadInfo
import io.github.osoykan.springkafka.example.domain.*
import io.github.osoykan.springkafka.example.infra.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

/**
 * Example order consumer demonstrating **hybrid DI injection**:
 *
 * - **Spring-managed beans**: [EventMetricsService], [EventValidator] - discovered via @Service/@Component
 * - **Koin-managed beans**: [OrderRepository], [NotificationService] - bridged via DependencyResolver
 *
 * This proves that Spring's component scanning works alongside the Koin dependency bridge,
 * allowing consumers to seamlessly use dependencies from both DI containers.
 *
 * Spring Kafka 3.2+ natively supports Kotlin suspend functions:
 * - The AckMode is automatically set to MANUAL for async handlers
 * - The message is acknowledged when the suspend function completes successfully
 * - If the handler throws, the message is not acknowledged and will be redelivered
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Async Returns</a>
 */
@Component
class OrderCreatedConsumer(
  // Spring-managed beans (auto-discovered via @Service/@Component)
  private val eventMetricsService: EventMetricsService,
  private val eventValidator: EventValidator,
  // Koin-managed beans (bridged via DependencyResolver)
  private val orderRepository: OrderRepository,
  private val notificationService: NotificationService
) {
  /**
   * Process an order creation event.
   *
   * This is a suspend function - Spring Kafka handles it natively:
   * - Runs the handler in a coroutine context
   * - Acknowledges only after successful completion
   * - Properly propagates exceptions
   */
  @KafkaListener(topics = ["example.orders.created"])
  suspend fun consume(record: ConsumerRecord<String, OrderCreatedEvent>) {
    dumpThreadInfo(javaClass.name)
    val event = record.value()

    logger.info {
      "Processing order: ${event.orderId} for customer ${event.customerId}, amount: ${event.amount} ${event.currency}"
    }

    // Validate using Spring-managed validator
    when (val result = eventValidator.validateOrderEvent(event.orderId, event.customerId)) {
      is ValidationResult.Valid -> { /* proceed */ }

      is ValidationResult.Invalid -> {
        logger.error { "Validation failed: ${result.errors}" }
        throw IllegalArgumentException("Invalid order: ${result.errors}")
      }
    }

    // Validate business rules
    validateOrder(event)

    // Save to repository (Koin-managed service!)
    orderRepository.save(event)

    // Send notification (Koin-managed service!)
    notificationService.sendOrderConfirmation(event)

    // Record metrics using Spring-managed service
    eventMetricsService.recordProcessed("OrderCreatedEvent")

    logger.info {
      "Successfully processed order: ${event.orderId} " +
        "(total orders: ${orderRepository.count()}, " +
        "total events: ${eventMetricsService.getProcessedCount()})"
    }
  }

  private fun validateOrder(event: OrderCreatedEvent) {
    require(event.amount > BigDecimal.ZERO) {
      "Order amount must be positive: ${event.amount}"
    }
    require(event.items.isNotEmpty()) {
      "Order must have at least one item"
    }
  }
}
