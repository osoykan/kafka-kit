package io.github.osoykan.kafkaflow

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Base marker interface for all Kafka consumers.
 *
 * Consumers are lightweight - they only implement business logic.
 * All retry, DLT, and metrics handling is done internally by the supervisor.
 */
interface Consumer<K, V> {
  /**
   * Name of this consumer, used for logging and metrics.
   * Defaults to the simple class name.
   */
  val consumerName: String
    get() = this::class.simpleName ?: "UnknownConsumer"
}

/**
 * Consumer with automatic acknowledgment.
 *
 * Just implement consume() - that's it!
 *
 * Internal handling (you don't need to care):
 * - Retries: Configured via @KafkaTopic annotation
 * - DLT: Handled automatically when retries exhausted
 * - Metrics: Recorded automatically
 * - Ack: Automatic on success
 *
 * If you need to handle DLT messages, create a separate DLT consumer.
 * If you need alerts on failures, use metrics (kafka.consumer.dlt counter).
 *
 * Example:
 * ```kotlin
 * @KafkaTopic(
 *     name = "orders.created",
 *     retry = "orders.created.retry",
 *     dlt = "orders.created.dlt"
 * )
 * class OrderCreatedConsumer(
 *     private val orderService: OrderService
 * ) : ConsumerAutoAck<String, OrderCreatedEvent> {
 *     override suspend fun consume(record: ConsumerRecord<String, OrderCreatedEvent>) {
 *         orderService.processOrder(record.value())
 *     }
 * }
 * ```
 */
interface ConsumerAutoAck<K, V> : Consumer<K, V> {
  /**
   * Process a record. That's it!
   *
   * @param record The Kafka consumer record to process
   * @throws Exception Any exception will trigger retry logic (if configured)
   */
  suspend fun consume(record: ConsumerRecord<K, V>)
}

/**
 * Consumer with manual acknowledgment.
 *
 * For when you need explicit control over commits.
 * Use this when you want to:
 * - Acknowledge only after external side effects complete
 * - Implement custom batching logic
 * - Control exactly when offsets are committed
 *
 * Example:
 * ```kotlin
 * @KafkaTopic(name = "payments")
 * class PaymentConsumer : ConsumerManualAck<String, PaymentEvent> {
 *     override suspend fun consume(record: ConsumerRecord<String, PaymentEvent>, ack: Acknowledgment) {
 *         paymentService.process(record.value())
 *         // Only acknowledge after payment is processed
 *         ack.acknowledge()
 *     }
 * }
 * ```
 */
interface ConsumerManualAck<K, V> : Consumer<K, V> {
  /**
   * Process a record with manual acknowledgment control.
   *
   * @param record The Kafka consumer record to process
   * @param ack Acknowledgment handle - call acknowledge() when done
   * @throws Exception Any exception will trigger retry logic (if configured)
   */
  suspend fun consume(record: ConsumerRecord<K, V>, ack: Acknowledgment)
}

/**
 * Acknowledgment handle for manual commit.
 */
interface Acknowledgment {
  /**
   * Acknowledges the message, triggering offset commit.
   */
  fun acknowledge()
}

/**
 * Adapter to convert Spring Kafka Acknowledgment to our interface.
 */
internal class SpringAcknowledgmentAdapter(
  private val springAck: org.springframework.kafka.support.Acknowledgment
) : Acknowledgment {
  override fun acknowledge() {
    springAck.acknowledge()
  }
}
