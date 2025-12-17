package io.github.osoykan.springkafka.example.domain

import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

/**
 * Base marker interface for all domain events.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
sealed interface DomainEvent {
  val eventId: String
  val timestamp: Instant
}

/**
 * Order created event.
 */
data class OrderCreatedEvent(
  override val eventId: String = UUID.randomUUID().toString(),
  override val timestamp: Instant = Instant.now(),
  val orderId: String,
  val customerId: String,
  val amount: BigDecimal,
  val currency: String = "USD",
  val items: List<OrderItem> = emptyList()
) : DomainEvent

data class OrderItem(
  val productId: String,
  val quantity: Int,
  val unitPrice: BigDecimal
)

/**
 * Payment event.
 */
data class PaymentEvent(
  override val eventId: String = UUID.randomUUID().toString(),
  override val timestamp: Instant = Instant.now(),
  val paymentId: String,
  val orderId: String,
  val amount: BigDecimal,
  val currency: String = "USD",
  val method: PaymentMethod = PaymentMethod.CREDIT_CARD
) : DomainEvent

enum class PaymentMethod {
  CREDIT_CARD,
  DEBIT_CARD,
  BANK_TRANSFER,
  PAYPAL
}

/**
 * Notification event.
 */
data class NotificationEvent(
  override val eventId: String = UUID.randomUUID().toString(),
  override val timestamp: Instant = Instant.now(),
  val notificationId: String,
  val userId: String,
  val type: NotificationType,
  val title: String,
  val message: String,
  val metadata: Map<String, String> = emptyMap()
) : DomainEvent

enum class NotificationType {
  EMAIL,
  SMS,
  PUSH
}
