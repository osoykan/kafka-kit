package io.github.osoykan.ktor.springkafka

import kotlin.reflect.KClass

/**
 * Interface for resolving dependencies from external DI containers.
 *
 * When Spring cannot find a bean in its context, it falls back to this resolver.
 * Implement this interface to integrate your DI container (Koin, Kodein, etc.).
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
 * }
 *
 * // Usage
 * install(Koin) {
 *   modules(appModule)
 * }
 *
 * install(SpringKafka) {
 *   dependencyResolver = KoinResolver(getKoin())
 * }
 * ```
 *
 * ## Kodein Example
 *
 * ```kotlin
 * class KodeinResolver(private val di: DI) : DependencyResolver {
 *   override fun <T : Any> resolve(type: KClass<T>): T? =
 *     di.instanceOrNull(type.java)
 *
 *   override fun <T : Any> resolve(type: KClass<T>, name: String): T? =
 *     di.instanceOrNull(tag = name, type = type.java)
 *
 *   override fun <T : Any> resolveAll(type: KClass<T>): List<T> =
 *     di.allInstances(type.java)
 *
 *   override fun canResolve(type: KClass<*>): Boolean =
 *     resolve(type) != null
 * }
 * ```
 *
 * ## How It Works
 *
 * 1. Spring Kafka listener needs a dependency (e.g., `OrderRepository`)
 * 2. Spring first looks in its own context
 * 3. If not found, Spring calls `dependencyResolver.resolve(OrderRepository::class)`
 * 4. Your resolver queries Koin/Kodein/etc. and returns the instance
 * 5. Spring injects it into your `@KafkaListener` consumer
 *
 * For `List<T>` dependencies, Spring calls `resolveAll()` to get all instances.
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
