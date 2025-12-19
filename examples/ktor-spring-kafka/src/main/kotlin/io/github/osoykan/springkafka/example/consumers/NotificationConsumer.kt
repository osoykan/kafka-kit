package io.github.osoykan.springkafka.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafka.toolkit.dumpThreadInfo
import io.github.osoykan.springkafka.example.domain.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

private val logger = KotlinLogging.logger {}

/**
 * Consumer for notifications using Spring Kafka's @KafkaListener with Kotlin suspend function.
 *
 * For external API calls, suspend functions are ideal as they allow
 * non-blocking I/O operations without blocking threads.
 *
 * @see <a href="https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html">Async Returns</a>
 */
@Component
class NotificationConsumer {
  /**
   * Process a notification event.
   *
   * This suspend function can call external notification services asynchronously.
   */
  @KafkaListener(topics = ["example.notifications"])
  suspend fun consume(record: ConsumerRecord<String, NotificationEvent>) {
    dumpThreadInfo(javaClass.name)
    val event = record.value()

    logger.info {
      "Sending ${event.type} notification to user ${event.userId}: ${event.title}"
    }

    // Process based on notification type
    sendNotification(event)

    logger.info { "Notification sent: ${event.notificationId}" }
  }

  private suspend fun sendNotification(event: NotificationEvent) {
    when (event.type) {
      NotificationType.EMAIL -> sendEmail(event)
      NotificationType.SMS -> sendSms(event)
      NotificationType.PUSH -> sendPush(event)
    }
  }

  private suspend fun sendEmail(event: NotificationEvent) {
    // In real app: call email service API
    logger.debug { "Sending email: ${event.title} to ${event.userId}" }
  }

  private suspend fun sendSms(event: NotificationEvent) {
    // In real app: call SMS gateway API
    logger.debug { "Sending SMS: ${event.message} to ${event.userId}" }
  }

  private suspend fun sendPush(event: NotificationEvent) {
    // In real app: call push notification service
    logger.debug { "Sending push notification: ${event.title}" }
  }
}
