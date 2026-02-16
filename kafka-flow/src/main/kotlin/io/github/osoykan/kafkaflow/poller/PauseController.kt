package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * Reason for pausing a Kafka container.
 */
enum class PauseReason {
  BACKPRESSURE,
  GAP_DETECTED
}

/**
 * Coordinates multiple pause/resume requesters for a Kafka container.
 *
 * Multiple subsystems (backpressure, gap detection) can independently pause the container.
 * The container is only resumed when ALL pause reasons are cleared. This prevents one
 * subsystem from resuming a container that another subsystem paused.
 *
 * @param pause Function to pause the container
 * @param resume Function to resume the container
 * @param topicName Topic name for logging
 */
internal class PauseController(
  private val pause: () -> Unit,
  private val resume: () -> Unit,
  private val topicName: String = ""
) {
  private val activeReasons = ConcurrentHashMap.newKeySet<PauseReason>()

  /**
   * Requests a pause for the given reason.
   * The container is paused on the first reason added.
   */
  fun requestPause(reason: PauseReason) {
    val wasEmpty = activeReasons.isEmpty()
    activeReasons.add(reason)
    if (wasEmpty && activeReasons.isNotEmpty()) {
      logger.info { "PauseController[$topicName]: Pausing container (reason: $reason)" }
      pause()
    } else {
      logger.debug { "PauseController[$topicName]: Added pause reason $reason (active: $activeReasons)" }
    }
  }

  /**
   * Clears a pause reason. The container resumes only when all reasons are cleared.
   */
  fun clearPause(reason: PauseReason) {
    activeReasons.remove(reason)
    if (activeReasons.isEmpty()) {
      logger.info { "PauseController[$topicName]: Resuming container (cleared: $reason)" }
      resume()
    } else {
      logger.debug { "PauseController[$topicName]: Cleared $reason but still paused (remaining: $activeReasons)" }
    }
  }

  /**
   * Returns true if there's an active pause for any reason.
   */
  fun isPaused(): Boolean = activeReasons.isNotEmpty()

  /**
   * Returns the set of active pause reasons.
   */
  fun activeReasons(): Set<PauseReason> = activeReasons.toSet()
}
