package io.github.osoykan.kafkaflow.example.api

import io.github.osoykan.kafkaflow.ConsumerEngine
import io.github.osoykan.kafkaflow.KafkaFlowFactory
import io.github.osoykan.kafkaflow.await
import io.github.osoykan.kafkaflow.example.domain.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.koin.ktor.ext.inject
import java.math.BigDecimal
import java.util.*

/**
 * Configure HTTP routes for the example application.
 */
fun Application.configureRouting() {
  val kafkaFactory by inject<KafkaFlowFactory<String, DomainEvent>>()
  val kafkaTemplate = kafkaFactory.kafkaTemplate()
  val consumerEngine by inject<ConsumerEngine<String, DomainEvent>>()

  routing {
    // Health check
    get("/") {
      call.respondText("Kafka Flow Ktor Example - Running!")
    }

    // Health endpoint with consumer status
    get("/health") {
      val status = mapOf(
        "status" to "UP",
        "consumers" to mapOf(
          "started" to consumerEngine.isStarted(),
          "active" to consumerEngine.activeSupervisorCount(),
          "names" to consumerEngine.consumerNames()
        )
      )
      call.respond(status)
    }

    // ─────────────────────────────────────────────────────────────
    // Test endpoints for producing strongly typed events
    // ─────────────────────────────────────────────────────────────

    route("/api/test") {
      // Produce a successful order
      post("/orders/success") {
        val orderId = UUID.randomUUID().toString()
        val event = OrderCreatedEvent(
          orderId = orderId,
          customerId = "customer-${UUID.randomUUID().toString().take(8)}",
          amount = BigDecimal("199.99"),
          currency = "USD",
          items = listOf(
            OrderItem(
              productId = "product-1",
              quantity = 2,
              unitPrice = BigDecimal("99.99")
            )
          )
        )
        kafkaTemplate.send("example.orders.created", orderId, event).await()
        call.respond(HttpStatusCode.Accepted, mapOf("orderId" to orderId, "message" to "Order sent"))
      }

      // Produce an order with empty items (will fail validation)
      post("/orders/fail-validation") {
        val orderId = UUID.randomUUID().toString()
        val event = OrderCreatedEvent(
          orderId = orderId,
          customerId = "customer-${UUID.randomUUID().toString().take(8)}",
          amount = BigDecimal("50.00"),
          items = emptyList() // Will fail validation
        )
        kafkaTemplate.send("example.orders.created", orderId, event).await()
        call.respond(
          HttpStatusCode.Accepted,
          mapOf("orderId" to orderId, "message" to "Order with validation error sent")
        )
      }

      // Produce a payment
      post("/payments") {
        val paymentId = UUID.randomUUID().toString()
        val event = PaymentEvent(
          paymentId = paymentId,
          orderId = "order-${UUID.randomUUID().toString().take(8)}",
          amount = BigDecimal("99.99"),
          currency = "USD",
          method = PaymentMethod.CREDIT_CARD
        )
        kafkaTemplate.send("example.payments", paymentId, event).await()
        call.respond(HttpStatusCode.Accepted, mapOf("paymentId" to paymentId, "message" to "Payment sent"))
      }

      // Produce a payment with negative amount (will fail validation)
      post("/payments/fail") {
        val paymentId = UUID.randomUUID().toString()
        val event = PaymentEvent(
          paymentId = paymentId,
          orderId = "order-${UUID.randomUUID().toString().take(8)}",
          amount = BigDecimal("-10.00"), // Negative - will fail
          method = PaymentMethod.BANK_TRANSFER
        )
        kafkaTemplate.send("example.payments", paymentId, event).await()
        call.respond(HttpStatusCode.Accepted, mapOf("paymentId" to paymentId, "message" to "Payment with failure sent"))
      }

      // Produce a notification
      post("/notifications") {
        val notificationId = UUID.randomUUID().toString()
        val event = NotificationEvent(
          notificationId = notificationId,
          userId = "user-${UUID.randomUUID().toString().take(8)}",
          type = NotificationType.EMAIL,
          title = "Welcome!",
          message = "Welcome to our platform!",
          metadata = mapOf("campaign" to "onboarding")
        )
        kafkaTemplate.send("example.notifications", notificationId, event).await()
        call.respond(
          HttpStatusCode.Accepted,
          mapOf("notificationId" to notificationId, "message" to "Notification sent")
        )
      }

      // Produce a push notification
      post("/notifications/push") {
        val notificationId = UUID.randomUUID().toString()
        val event = NotificationEvent(
          notificationId = notificationId,
          userId = "user-${UUID.randomUUID().toString().take(8)}",
          type = NotificationType.PUSH,
          title = "New Order",
          message = "Your order has been shipped!"
        )
        kafkaTemplate.send("example.notifications", notificationId, event).await()
        call.respond(HttpStatusCode.Accepted, mapOf("notificationId" to notificationId, "message" to "Push notification sent"))
      }
    }
  }
}
