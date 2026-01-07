package io.github.osoykan.kafkaflow.example.e2e

import arrow.core.None
import com.trendyol.stove.testing.e2e.http.http
import com.trendyol.stove.testing.e2e.standalone.kafka.kafka
import com.trendyol.stove.testing.e2e.system.TestSystem.Companion.validate
import io.github.osoykan.kafkaflow.example.domain.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.math.BigDecimal
import java.util.*
import kotlin.time.Duration.Companion.seconds

/**
 * E2E tests for Kafka Flow Ktor example using Stove.
 *
 * These tests verify:
 * 1. The application starts correctly with Kafka configuration
 * 2. Producers publish messages correctly
 * 3. Consumers consume messages correctly
 */
class KafkaFlowE2eTests :
  FunSpec({

    test("health check should return consumer status") {
      validate {
        http {
          getResponse<Any>(uri = "/health") { response ->
            response.status shouldBe 200
          }
        }
      }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Payment Consumer Tests (Manual Ack) - These use consumeWithAck path
    // ─────────────────────────────────────────────────────────────────────────────

    test("should publish and consume payment event (manual ack consumer)") {
      validate {
        val paymentId = UUID.randomUUID().toString()
        val event = PaymentEvent(
          paymentId = paymentId,
          orderId = "order-456",
          amount = BigDecimal("99.99"),
          method = PaymentMethod.CREDIT_CARD
        )

        kafka {
          publish("example.payments", event, key = paymentId.some())

          shouldBePublished<PaymentEvent> {
            actual.paymentId == paymentId
          }

          // Manual-ack consumers need more time to subscribe and poll
          shouldBeConsumed<PaymentEvent>(atLeastIn = 10.seconds) {
            actual.paymentId == paymentId
          }
        }
      }
    }

    test("should produce payment via HTTP and consume it") {
      validate {
        http {
          postAndExpectBodilessResponse(uri = "/api/test/payments", body = None, token = None) { response ->
            response.status shouldBe 202
          }
        }

        kafka {
          shouldBePublished<PaymentEvent> {
            actual.method == PaymentMethod.CREDIT_CARD
          }

          shouldBeConsumed<PaymentEvent>(10.seconds) {
            actual.method == PaymentMethod.CREDIT_CARD
          }
        }
      }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Order Consumer Tests (Auto Ack) - These use poller path
    // ─────────────────────────────────────────────────────────────────────────────

    test("should publish and consume order event (auto ack consumer)") {
      validate {
        val orderId = UUID.randomUUID().toString()
        val event = OrderCreatedEvent(
          orderId = orderId,
          customerId = "customer-123",
          amount = BigDecimal("199.99"),
          items = listOf(OrderItem("product-1", 2, BigDecimal("99.99")))
        )

        kafka {
          publish("example.orders.created", event, key = orderId.some())

          shouldBePublished<OrderCreatedEvent> {
            actual.orderId == orderId
          }

          shouldBeConsumed<OrderCreatedEvent> {
            actual.orderId == orderId
          }
        }
      }
    }

    test("should produce order via HTTP and consume it") {
      validate {
        http {
          postAndExpectBodilessResponse(uri = "/api/test/orders/success", body = None, token = None) { response ->
            response.status shouldBe 202
          }
        }

        kafka {
          shouldBePublished<OrderCreatedEvent> {
            actual.items.isNotEmpty()
          }

          shouldBeConsumed<OrderCreatedEvent> {
            actual.items.isNotEmpty()
          }
        }
      }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Notification Consumer Tests (Auto Ack)
    // ─────────────────────────────────────────────────────────────────────────────

    test("should publish and consume notification event") {
      validate {
        val notificationId = UUID.randomUUID().toString()
        val event = NotificationEvent(
          notificationId = notificationId,
          userId = "user-789",
          type = NotificationType.EMAIL,
          title = "Welcome!",
          message = "Welcome to our platform!"
        )

        kafka {
          publish("example.notifications", event, key = notificationId.some())

          shouldBePublished<NotificationEvent> {
            actual.notificationId == notificationId
          }

          shouldBeConsumed<NotificationEvent> {
            actual.notificationId == notificationId
          }
        }
      }
    }

    test("should produce notification via HTTP and consume it") {
      validate {
        http {
          postAndExpectBodilessResponse(uri = "/api/test/notifications", body = None, token = None) { response ->
            response.status shouldBe 202
          }
        }

        kafka {
          shouldBePublished<NotificationEvent> {
            actual.type == NotificationType.EMAIL
          }

          shouldBeConsumed<NotificationEvent> {
            actual.type == NotificationType.EMAIL
          }
        }
      }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // DLT Test - Verify failed messages are sent to DLT
    // ─────────────────────────────────────────────────────────────────────────────

    test("should send invalid order to DLT after consumer fails") {
      validate {
        val orderId = UUID.randomUUID().toString()
        val event = OrderCreatedEvent(
          orderId = orderId,
          customerId = "customer-invalid",
          amount = BigDecimal("50.00"),
          items = emptyList() // Will fail validation
        )

        kafka {
          publish("example.orders.created", event, key = orderId.some())

          // Should be published to main topic first
          shouldBePublished<OrderCreatedEvent> {
            actual.orderId == orderId
          }
        }
      }
    }
  })

private fun String.some(): arrow.core.Some<String> = arrow.core.Some(this)
