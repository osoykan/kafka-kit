package io.github.osoykan.springkafka.example.infra

import io.github.osoykan.ktorkafka.DependencyResolver
import org.koin.core.Koin
import org.koin.core.qualifier.named
import kotlin.reflect.KClass

/**
 * DependencyResolver implementation that bridges Koin with Spring Kafka.
 *
 * This allows Spring Kafka listeners (annotated with @KafkaListener) to
 * inject dependencies managed by Koin - the primary DI container in Ktor apps.
 *
 * ## How it works
 *
 * When Spring needs to autowire a dependency into a @KafkaListener consumer:
 * 1. Spring first checks its own context
 * 2. If not found, Spring falls back to this resolver via FallbackBeanFactory
 * 3. This resolver queries Koin for the dependency
 * 4. If found, Spring caches it and injects it
 *
 * ## Example
 *
 * ```kotlin
 * // Koin module
 * module {
 *   single { OrderRepository() }
 *   single { NotificationService() }
 * }
 *
 * // Spring Kafka consumer can now inject Koin beans
 * @Component
 * class OrderConsumer(
 *   private val orderRepository: OrderRepository,      // Injected from Koin!
 *   private val notificationService: NotificationService // Injected from Koin!
 * ) {
 *   @KafkaListener(topics = ["orders"])
 *   suspend fun consume(record: ConsumerRecord<String, OrderEvent>) {
 *     orderRepository.save(record.value())
 *     notificationService.notify(...)
 *   }
 * }
 * ```
 */
class KoinDependencyResolver(
  private val koin: Koin
) : DependencyResolver {
  override fun <T : Any> resolve(type: KClass<T>): T? = koin.getOrNull(type, null, null)

  override fun <T : Any> resolve(type: KClass<T>, name: String): T? = koin.getOrNull(type, named(name), null)

  override fun <T : Any> resolveAll(type: KClass<T>): List<T> {
    // Koin's getAll() requires reified type, but we can use scope-based resolution
    // For simplicity, we return single bean as list or empty if not found
    val single = resolve(type)
    return if (single != null) listOf(single) else emptyList()
  }

  override fun canResolve(type: KClass<*>): Boolean =
    runCatching { koin.get<Any>(type, null, null) }.isSuccess
}
