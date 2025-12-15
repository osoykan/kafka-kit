package io.github.osoykan.kafkaflow.example.api

import io.github.osoykan.kafkaflow.ConsumerEngine
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.koin.ktor.ext.inject
import org.springframework.kafka.core.KafkaTemplate
import java.util.*

/**
 * Configure HTTP routes for the example application.
 */
fun Application.configureRouting() {
  val kafkaTemplate by inject<KafkaTemplate<String, String>>()
  val consumerEngine by inject<ConsumerEngine<String, String>>()

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
    // Test endpoints for producing messages
    // ─────────────────────────────────────────────────────────────

    route("/api/test") {
      // Produce a successful order
      post("/orders/success") {
        val orderId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.orders.created", orderId, """{"status":"success"}""")
        call.respond(HttpStatusCode.Accepted, mapOf("orderId" to orderId, "message" to "Order sent"))
      }

      // Produce an order that will fail temporarily (retried)
      post("/orders/fail-temp") {
        val orderId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.orders.created", orderId, """{"status":"fail-temporary"}""")
        call.respond(HttpStatusCode.Accepted, mapOf("orderId" to orderId, "message" to "Order with temporary failure sent"))
      }

      // Produce an order that will fail validation (goes to DLT immediately)
      post("/orders/fail-validation") {
        val orderId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.orders.created", orderId, """{"status":"fail-validation"}""")
        call.respond(HttpStatusCode.Accepted, mapOf("orderId" to orderId, "message" to "Order with validation error sent"))
      }

      // Produce a payment
      post("/payments") {
        val paymentId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.payments", paymentId, """{"amount":99.99}""")
        call.respond(HttpStatusCode.Accepted, mapOf("paymentId" to paymentId, "message" to "Payment sent"))
      }

      // Produce a payment that fails
      post("/payments/fail") {
        val paymentId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.payments", paymentId, """{"status":"insufficient-funds"}""")
        call.respond(HttpStatusCode.Accepted, mapOf("paymentId" to paymentId, "message" to "Payment with failure sent"))
      }

      // Produce a notification
      post("/notifications") {
        val notificationId = UUID.randomUUID().toString()
        kafkaTemplate.send("example.notifications", notificationId, """{"type":"email"}""")
        call.respond(HttpStatusCode.Accepted, mapOf("notificationId" to notificationId, "message" to "Notification sent"))
      }
    }
  }
}
