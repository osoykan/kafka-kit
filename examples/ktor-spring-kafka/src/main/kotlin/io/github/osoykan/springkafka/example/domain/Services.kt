package io.github.osoykan.springkafka.example.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * Repository for storing and retrieving orders.
 * This is a Koin-managed service that will be injected into Spring Kafka consumers.
 */
class OrderRepository {
  private val orders = ConcurrentHashMap<String, OrderCreatedEvent>()

  suspend fun save(order: OrderCreatedEvent) {
    orders[order.orderId] = order
    logger.info { "Saved order ${order.orderId} to repository (total orders: ${orders.size})" }
  }

  suspend fun findById(orderId: String): OrderCreatedEvent? = orders[orderId]

  suspend fun findByCustomerId(customerId: String): List<OrderCreatedEvent> =
    orders.values.filter { it.customerId == customerId }

  fun count(): Int = orders.size
}

/**
 * Service for sending notifications.
 * This is a Koin-managed service that will be injected into Spring Kafka consumers.
 */
class NotificationService {
  private val sentNotifications = mutableListOf<SentNotification>()

  suspend fun sendOrderConfirmation(order: OrderCreatedEvent) {
    val notification = SentNotification(
      type = NotificationType.EMAIL,
      userId = order.customerId,
      subject = "Order Confirmation: ${order.orderId}",
      body = "Your order of ${order.amount} ${order.currency} has been received."
    )
    sentNotifications.add(notification)
    logger.info { "Sent order confirmation to customer ${order.customerId} for order ${order.orderId}" }
  }

  suspend fun sendPaymentConfirmation(payment: PaymentEvent) {
    val notification = SentNotification(
      type = NotificationType.EMAIL,
      userId = "customer", // In real app, look up customer from payment
      subject = "Payment Received: ${payment.paymentId}",
      body = "Payment of ${payment.amount} ${payment.currency} received for order ${payment.orderId}."
    )
    sentNotifications.add(notification)
    logger.info { "Sent payment confirmation for payment ${payment.paymentId}" }
  }

  fun getSentNotifications(): List<SentNotification> = sentNotifications.toList()
}

data class SentNotification(
  val type: NotificationType,
  val userId: String,
  val subject: String,
  val body: String
)
