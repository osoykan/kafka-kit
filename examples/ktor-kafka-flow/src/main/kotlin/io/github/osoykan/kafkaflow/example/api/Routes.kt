package io.github.osoykan.kafkaflow.example.api

import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.domain.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.apache.kafka.clients.producer.ProducerRecord
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
  val producer by inject<FlowKafkaProducer<String, DomainEvent>>()

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

      // ─────────────────────────────────────────────────────────────
      // Inventory batch endpoints — demonstrate BatchConsumer + FlowKafkaProducer
      // ─────────────────────────────────────────────────────────────

      // Produce a single inventory event
      post("/inventory") {
        val sku = "SKU-${UUID.randomUUID().toString().take(8)}"
        val event = InventoryEvent(
          sku = sku,
          warehouseId = "warehouse-1",
          quantityChange = 10,
          reason = InventoryReason.RESTOCK
        )
        producer.send("example.inventory", sku, event)
        call.respond(HttpStatusCode.Accepted, mapOf("sku" to sku, "message" to "Inventory event sent"))
      }

      // Produce a batch of inventory events using parallel send
      post("/inventory/batch") {
        val warehouses = listOf("warehouse-1", "warehouse-2", "warehouse-3")
        val records = (1..5).map { i ->
          val sku = "SKU-BATCH-$i"
          val event = InventoryEvent(
            sku = sku,
            warehouseId = warehouses[i % warehouses.size],
            quantityChange = i * 5,
            reason = if (i % 2 == 0) InventoryReason.RESTOCK else InventoryReason.SALE
          )
          ProducerRecord<String, DomainEvent>("example.inventory", sku, event)
        }
        producer.sendAllParallel(records)
        call.respond(HttpStatusCode.Accepted, mapOf("message" to "Batch inventory events sent in parallel"))
      }

      // Produce a batch with result tracking (some may have blank SKU → will fail at consumer)
      post("/inventory/batch-with-results") {
        val records = (1..5).map { i ->
          val sku = if (i == 3) "" else "SKU-RESULT-$i" // blank SKU will fail consumer validation
          val event = InventoryEvent(
            sku = sku,
            warehouseId = "warehouse-1",
            quantityChange = i,
            reason = InventoryReason.ADJUSTMENT
          )
          ProducerRecord<String, DomainEvent>("example.inventory", sku, event)
        }
        val results = producer.sendAllParallel(records)
        val summary = results.map { result ->
          mapOf(
            "status" to "sent",
            "topic" to result.topic(),
            "partition" to result.partition()
          )
        }
        call.respond(HttpStatusCode.Accepted, mapOf("results" to summary))
      }
    }
  }
}
