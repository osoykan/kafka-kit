package io.github.osoykan.springkafka.example.infra

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Spring-managed service for tracking event processing metrics.
 *
 * This bean is discovered by Spring's component scanning and lives in the
 * Spring context. It demonstrates that Spring's auto-discovery works alongside
 * the Koin dependency bridge.
 *
 * This service will be injected into Kafka consumers alongside Koin-managed beans,
 * proving the hybrid DI setup works correctly.
 */
@Service
class EventMetricsService {
  private val processedCount = AtomicLong(0)
  private val lastProcessedAt = ConcurrentHashMap<String, Instant>()

  fun recordProcessed(eventType: String) {
    val count = processedCount.incrementAndGet()
    lastProcessedAt[eventType] = Instant.now()
    logger.info { "[Spring Bean] Event processed: $eventType (total: $count)" }
  }

  fun getProcessedCount(): Long = processedCount.get()

  fun getLastProcessedAt(eventType: String): Instant? = lastProcessedAt[eventType]

  fun getStats(): Map<String, Any> = mapOf(
    "totalProcessed" to processedCount.get(),
    "lastProcessedByType" to lastProcessedAt.toMap()
  )
}

/**
 * Spring-managed component for validating events.
 *
 * Another example of a Spring @Component that will be auto-discovered
 * and can be injected into Kafka consumers.
 */
@Component
class EventValidator {
  fun validateOrderEvent(orderId: String, customerId: String): ValidationResult {
    val errors = mutableListOf<String>()

    if (orderId.isBlank()) {
      errors.add("Order ID cannot be blank")
    }
    if (customerId.isBlank()) {
      errors.add("Customer ID cannot be blank")
    }

    return if (errors.isEmpty()) {
      logger.debug { "[Spring Bean] Event validation passed for order: $orderId" }
      ValidationResult.Valid
    } else {
      logger.warn { "[Spring Bean] Event validation failed: $errors" }
      ValidationResult.Invalid(errors)
    }
  }
}

sealed class ValidationResult {
  data object Valid : ValidationResult()

  data class Invalid(
    val errors: List<String>
  ) : ValidationResult()
}
