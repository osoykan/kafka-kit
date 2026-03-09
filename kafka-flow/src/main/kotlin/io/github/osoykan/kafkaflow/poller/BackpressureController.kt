package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.BackpressureConfig
import kotlinx.coroutines.channels.Channel
import kotlin.math.max
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * Controls backpressure by pausing/resuming the Kafka container based on buffer fill level.
 *
 * When the buffer fills beyond [BackpressureConfig.pauseThreshold], the container is paused
 * to stop consuming more records. When it drains below [BackpressureConfig.resumeThreshold],
 * consumption resumes.
 *
 * @param containerProvider Lazy provider for the container (allows late initialization)
 * @param config Backpressure configuration with thresholds
 * @param bufferCapacity Total buffer capacity to calculate thresholds against
 * @param topicName Topic name for logging
 */
internal class BackpressureController(
  private val pauseController: PauseController,
  private val config: BackpressureConfig,
  private val bufferCapacity: Int,
  private val topicName: String
) {
  private val bufferCount = AtomicInteger(0)
  private val paused = AtomicBoolean(false)
  private val effectiveBufferCapacity = bufferCapacity.normalizeBufferCapacity()

  private val pauseThresholdCount = max(1, (effectiveBufferCapacity * config.pauseThreshold).toInt())
  private val resumeThresholdCount = max(0, (effectiveBufferCapacity * config.resumeThreshold).toInt())

  /**
   * Called when a record is added to the buffer.
   * May pause the container if buffer exceeds pause threshold.
   */
  fun onBufferAdd() {
    if (!config.enabled) return

    val count = bufferCount.incrementAndGet()
    if (count >= pauseThresholdCount && paused.compareAndSet(false, true)) {
      pauseController.requestPause(PauseReason.BACKPRESSURE)
      logger.info { "Backpressure: Requested pause for $topicName (buffer: $count/$effectiveBufferCapacity)" }
    }
  }

  /**
   * Called when a record is consumed from the buffer.
   * May resume the container if buffer drops below resume threshold.
   */
  fun onBufferConsume() {
    if (!config.enabled) return

    val count = bufferCount.decrementAndGet()
    if (count <= resumeThresholdCount && paused.compareAndSet(true, false)) {
      pauseController.clearPause(PauseReason.BACKPRESSURE)
      logger.info { "Backpressure: Cleared pause for $topicName (buffer: $count/$effectiveBufferCapacity)" }
    }
  }
}

private fun Int.normalizeBufferCapacity(): Int = when {
  this == Channel.RENDEZVOUS -> 1
  this <= 0 -> DEFAULT_BUFFERED_CHANNEL_CAPACITY
  else -> this
}

private const val DEFAULT_BUFFERED_CHANNEL_CAPACITY = 64
