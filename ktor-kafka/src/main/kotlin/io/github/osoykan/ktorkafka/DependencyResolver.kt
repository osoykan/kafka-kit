package io.github.osoykan.ktorkafka

import kotlin.reflect.KClass

/**
 * Interface for resolving dependencies from external DI containers.
 *
 * When Spring cannot find a bean in its context, it falls back to this resolver.
 * Implement this interface to integrate your DI container (Koin, Kodein, etc.).
 *
 * ## Important: Singleton Scope Required
 *
 * **Only singleton-scoped beans should be resolved for Kafka listeners.**
 *
 * Kafka listeners are singletons - they're created once and live for the application
 * lifetime. If a listener depends on a scoped bean (request, session, etc.), that
 * bean is captured at construction time and never refreshed, leading to:
 * - Stale state across messages
 * - Thread safety issues
 * - Resource leaks
 *
 * Implement [isSingleton] to enable scope validation. If a non-singleton is resolved,
 * a warning is logged. For scoped dependencies, use a factory/provider pattern instead.
 *
 * ## Koin Example
 *
 * ```kotlin
 * class KoinResolver(private val koin: Koin) : DependencyResolver {
 *   override fun <T : Any> resolve(type: KClass<T>): T? =
 *     koin.getOrNull(type)
 *
 *   override fun <T : Any> resolve(type: KClass<T>, name: String): T? =
 *     koin.getOrNull(type, named(name))
 *
 *   override fun <T : Any> resolveAll(type: KClass<T>): List<T> =
 *     koin.getAll(type)
 *
 *   override fun canResolve(type: KClass<*>): Boolean =
 *     koin.getOrNull(type) != null
 *
 *   override fun isSingleton(type: KClass<*>): Boolean {
 *     // Koin: check if definition is a singleton
 *     val definition = koin.getScope(ScopeID.ROOT).beanRegistry
 *       .getAllDefinitions().find { it.primaryType == type }
 *     return definition?.kind == Kind.Singleton
 *   }
 * }
 * ```
 *
 * ## How It Works
 *
 * 1. Spring Kafka listener needs a dependency (e.g., `OrderRepository`)
 * 2. Spring first looks in its own context
 * 3. If not found, Spring calls `dependencyResolver.resolve(OrderRepository::class)`
 * 4. Resolver checks [isSingleton] - warns if non-singleton
 * 5. Your resolver queries Koin/Kodein/etc. and returns the instance
 * 6. Spring injects it into your `@KafkaListener` consumer
 *
 * **Note:** Resolved beans are NOT registered as Spring singletons to avoid
 * double ownership. The external DI container remains the sole owner.
 */
interface DependencyResolver {
  /**
   * Resolve a dependency by type.
   *
   * @param type The class to resolve
   * @return The resolved instance, or null if not found
   */
  fun <T : Any> resolve(type: KClass<T>): T?

  /**
   * Resolve a named/qualified dependency by type.
   *
   * @param type The class to resolve
   * @param name The qualifier/name of the dependency
   * @return The resolved instance, or null if not found
   */
  fun <T : Any> resolve(type: KClass<T>, name: String): T?

  /**
   * Resolve all dependencies of the given type.
   *
   * Used for `List<T>` injection where Spring needs all beans of a type.
   *
   * @param type The class to resolve
   * @return List of all instances of the type, empty if none found
   */
  fun <T : Any> resolveAll(type: KClass<T>): List<T>

  /**
   * Check if this resolver can provide a dependency of the given type.
   *
   * @param type The class to check
   * @return true if this resolver can provide the dependency
   */
  fun canResolve(type: KClass<*>): Boolean

  /**
   * Check if the dependency is a singleton in the external DI container.
   *
   * Kafka listeners are singletons, so they should only depend on singleton beans.
   * Non-singleton dependencies (request-scoped, session-scoped, etc.) will be
   * captured at listener construction time and never refreshed.
   *
   * Override this to enable scope validation. Default returns true (assumes singleton).
   *
   * @param type The class to check
   * @return true if the dependency is singleton-scoped, false otherwise
   */
  fun isSingleton(type: KClass<*>): Boolean = true
}

/**
 * Default no-op resolver - never resolves anything.
 * Used when no external DI container is configured.
 */
object NoOpDependencyResolver : DependencyResolver {
  override fun <T : Any> resolve(type: KClass<T>): T? = null

  override fun <T : Any> resolve(type: KClass<T>, name: String): T? = null

  override fun <T : Any> resolveAll(type: KClass<T>): List<T> = emptyList()

  override fun canResolve(type: KClass<*>): Boolean = false
}

/** Resolve with reified type */
inline fun <reified T : Any> DependencyResolver.resolve(): T? = resolve(T::class)

/** Resolve named dependency with reified type */
inline fun <reified T : Any> DependencyResolver.resolve(name: String): T? = resolve(T::class, name)

/** Resolve all instances of type */
inline fun <reified T : Any> DependencyResolver.resolveAll(): List<T> = resolveAll(T::class)

/** Check if type can be resolved */
inline fun <reified T : Any> DependencyResolver.canResolve(): Boolean = canResolve(T::class)
