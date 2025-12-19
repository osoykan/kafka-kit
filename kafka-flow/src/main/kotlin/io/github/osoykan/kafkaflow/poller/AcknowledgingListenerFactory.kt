package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.CompletionEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.support.Acknowledgment

private val logger = KotlinLogging.logger {}

/**
 * Callback invoked when a record is successfully emitted to the flow.
 */
internal typealias OnRecordEmitted = () -> Unit

/**
 * Callback invoked when a record fails to be emitted.
 */
internal typealias OnRecordEmitFailed = (Exception) -> Unit

/**
 * Callback invoked when a record is acknowledged (processing completed).
 */
internal typealias OnRecordAcknowledged = () -> Unit

/**
 * Factory for creating Spring Kafka [AcknowledgingMessageListener] instances
 * that integrate with the ordered commit system.
 *
 * The created listeners:
 * 1. Wrap each record in an [AckableRecord]
 * 2. Route acknowledgments through a [CompletionEvent] channel for ordered commits
 * 3. Emit records to a provided send function
 */
internal object AcknowledgingListenerFactory {
  /**
   * Creates an acknowledging message listener that routes acks through ordered commits.
   *
   * @param commitChannel Channel for sending completion events to the ordered committer
   * @param sendToFlow Function to send the ackable record to the flow
   * @param onRecordEmitted Called after a record is successfully sent to the flow (buffer add)
   * @param onRecordAcknowledged Called when acknowledge() is invoked (processing complete)
   * @param onRecordEmitFailed Called when sending to the flow fails
   */
  fun <K : Any, V : Any> create(
    commitChannel: Channel<CompletionEvent>,
    sendToFlow: (AckableRecord<K, V>) -> Result<Unit>,
    onRecordEmitted: OnRecordEmitted = {},
    onRecordAcknowledged: OnRecordAcknowledged = {},
    onRecordEmitFailed: OnRecordEmitFailed = {}
  ): AcknowledgingMessageListener<K, V> = AcknowledgingMessageListener { record, ack ->
    if (ack != null) {
      val ackableRecord = createAckableRecord(record, ack, commitChannel, onRecordAcknowledged)
      sendToFlow(ackableRecord)
        .onSuccess { onRecordEmitted() }
        .onFailure { exception ->
          if (exception !is CancellationException) {
            logger.error(exception) { "Failed to emit record from topic ${record.topic()}: ${record.key()}" }
            onRecordEmitFailed(exception as Exception)
          }
        }
    }
  }

  /**
   * Creates an [AckableRecord] that routes acknowledgment through the commit channel.
   */
  private fun <K, V> createAckableRecord(
    record: ConsumerRecord<K, V>,
    ack: Acknowledgment,
    commitChannel: Channel<CompletionEvent>,
    onRecordAcknowledged: OnRecordAcknowledged
  ): AckableRecord<K, V> = AckableRecord(
    record = record,
    acknowledge = {
      // Signal processing complete for backpressure
      onRecordAcknowledged()

      val event = CompletionEvent(
        partition = record.partition(),
        offset = record.offset(),
        acknowledge = { ack.acknowledge() }
      )
      val result = commitChannel.trySend(event)
      if (result.isFailure) {
        logger.warn {
          "Failed to send completion event for ${record.topic()}:${record.partition()}:${record.offset()}"
        }
        // Fallback to direct ack if channel fails (shouldn't happen with UNLIMITED)
        ack.acknowledge()
      }
    }
  )
}
