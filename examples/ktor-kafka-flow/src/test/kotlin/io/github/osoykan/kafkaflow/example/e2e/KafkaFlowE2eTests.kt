package io.github.osoykan.kafkaflow.example.e2e

import arrow.core.None
import com.trendyol.stove.http.http
import com.trendyol.stove.kafka.kafka
import com.trendyol.stove.system.stove
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
      stove {
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
      stove {
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
      stove {
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
      stove {
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
      stove {
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
      stove {
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
      stove {
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
    // Inventory Batch Consumer Tests
    // ─────────────────────────────────────────────────────────────────────────────

    test("should publish and consume inventory event via batch consumer") {
      stove {
        val sku = "SKU-${UUID.randomUUID().toString().take(8)}"
        val event = InventoryEvent(
          sku = sku,
          warehouseId = "warehouse-1",
          quantityChange = 10,
          reason = InventoryReason.RESTOCK
        )

        kafka {
          publish("example.inventory", event, key = sku.some())

          shouldBePublished<InventoryEvent> {
            actual.sku == sku
          }

          shouldBeConsumed<InventoryEvent>(atLeastIn = 10.seconds) {
            actual.sku == sku
          }
        }
      }
    }

    test("should produce inventory batch via HTTP and consume them") {
      stove {
        http {
          postAndExpectBodilessResponse(uri = "/api/test/inventory/batch", body = None, token = None) { response ->
            response.status shouldBe 202
          }
        }

        kafka {
          shouldBePublished<InventoryEvent> {
            actual.sku.startsWith("SKU-BATCH-")
          }

          shouldBeConsumed<InventoryEvent>(10.seconds) {
            actual.sku.startsWith("SKU-BATCH-")
          }
        }
      }
    }

    test("should send inventory event with blank SKU to DLT via batch failure handler") {
      stove {
        val event = InventoryEvent(
          sku = "",
          warehouseId = "warehouse-1",
          quantityChange = 5,
          reason = InventoryReason.ADJUSTMENT
        )

        kafka {
          publish("example.inventory", event, key = "blank-sku".some())

          shouldBePublished<InventoryEvent> {
            actual.sku == ""
          }
        }
      }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // DLT Test - Verify failed messages are sent to DLT
    // ─────────────────────────────────────────────────────────────────────────────

    test("should send invalid order to DLT after consumer fails") {
      stove {
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
