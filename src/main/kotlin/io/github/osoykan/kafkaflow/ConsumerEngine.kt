package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Manages the lifecycle of all consumers.
 *
 * Discovers consumers, creates supervisors, and handles start/stop.
 * Integrates with application lifecycle (e.g., Ktor, Spring).
 *
 * Example usage with Ktor:
 * ```kotlin
 * fun Application.module() {
 *     val engine = get<ConsumerEngine<String, Any>>()
 *
 *     monitor.subscribe(ApplicationStarted) { engine.start() }
 *     monitor.subscribe(ApplicationStopPreparing) { engine.stop() }
 * }
 * ```
 */
class ConsumerEngine<K : Any, V : Any>(
  private val consumers: List<Consumer<K, V>>,
  private val supervisorFactory: ConsumerSupervisorFactory<K, V>,
  private val metrics: KafkaFlowMetrics = NoOpMetrics,
  private val enabled: Boolean = true
) {
  private val supervisors = mutableListOf<ConsumerSupervisor>()
  private var started = false

  /**
   * Starts all registered consumers.
   */
  fun start() {
    if (!enabled) {
      logger.info { "Consumer engine disabled, skipping start" }
      return
    }

    if (started) {
      logger.warn { "Consumer engine already started" }
      return
    }

    logger.info { "Starting consumer engine with ${consumers.size} consumers" }

    supervisorFactory
      .createSupervisors(consumers)
      .also { supervisors.addAll(it) }
      .forEach { supervisor ->
        try {
          supervisor.start()
          metrics.recordConsumerStarted(supervisor.consumerName, supervisor.topics)
          logger.info { "Started consumer: ${supervisor.consumerName} for topics: ${supervisor.topics}" }
        } catch (e: Exception) {
          logger.error(e) { "Failed to start consumer: ${supervisor.consumerName}" }
        }
      }

    started = true
    logger.info { "Consumer engine started with ${supervisors.size} supervisors" }
  }

  /**
   * Stops all consumers gracefully.
   */
  fun stop() {
    if (!started) {
      logger.debug { "Consumer engine not started, skipping stop" }
      return
    }

    logger.info { "Stopping consumer engine with ${supervisors.size} supervisors" }

    supervisors.forEach { supervisor ->
      try {
        supervisor.stop()
        metrics.recordConsumerStopped(supervisor.consumerName)
        logger.info { "Stopped consumer: ${supervisor.consumerName}" }
      } catch (e: Exception) {
        logger.error(e) { "Error stopping consumer: ${supervisor.consumerName}" }
      }
    }

    supervisors.clear()
    started = false
    logger.info { "Consumer engine stopped" }
  }

  /**
   * Checks if the engine is started.
   */
  fun isStarted(): Boolean = started

  /**
   * Gets the count of active supervisors.
   */
  fun activeSupervisorCount(): Int = supervisors.count { it.isRunning() }

  /**
   * Gets all registered consumer names.
   */
  fun consumerNames(): List<String> = supervisors.map { it.consumerName }
}
