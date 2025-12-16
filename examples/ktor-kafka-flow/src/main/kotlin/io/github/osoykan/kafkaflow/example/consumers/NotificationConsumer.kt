package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.domain.*
import org.apache.kafka.clients.consumer.ConsumerRecord

private val logger = KotlinLogging.logger {}

/**
 * Consumer for notifications - demonstrates ALWAYS_RETRY classifier with strongly typed events.
 *
 * The consumer receives [io.github.osoykan.kafkaflow.example.domain.NotificationEvent] directly - no manual deserialization needed!
 *
 * For external API calls, you often want to retry all exceptions
 * because even validation-like errors might be transient on the remote side.
 */
@KafkaTopic(
  name = "example.notifications",
  retry = "example.notifications.retry",
  dlt = "example.notifications.dlt",
  concurrency = 4,
  maxInMemoryRetries = 3,
  maxRetryTopicAttempts = 5, // More attempts for notifications
  classifier = ClassifierType.ALWAYS_RETRY
)
class NotificationConsumer : ConsumerAutoAck<String, NotificationEvent> {
  override suspend fun consume(record: ConsumerRecord<String, NotificationEvent>) {
    val event = record.value()

    logger.info {
      "Sending ${event.type} notification to user ${event.userId}: ${event.title}"
    }

    // Process based on notification type
    sendNotification(event)

    logger.info { "Notification sent: ${event.notificationId}" }
  }

  private fun sendNotification(event: NotificationEvent) {
    when (event.type) {
      NotificationType.EMAIL -> sendEmail(event)
      NotificationType.SMS -> sendSms(event)
      NotificationType.PUSH -> sendPush(event)
    }
  }

  private fun sendEmail(event: NotificationEvent) {
    logger.debug { "Sending email: ${event.title} to ${event.userId}" }
  }

  private fun sendSms(event: NotificationEvent) {
    logger.debug { "Sending SMS: ${event.message} to ${event.userId}" }
  }

  private fun sendPush(event: NotificationEvent) {
    logger.debug { "Sending push notification: ${event.title}" }
  }
}
