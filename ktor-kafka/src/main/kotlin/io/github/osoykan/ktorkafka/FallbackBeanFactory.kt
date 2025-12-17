package io.github.osoykan.ktorkafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.config.DependencyDescriptor
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.context.annotation.ContextAnnotationAutowireCandidateResolver
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * A Spring BeanFactory that delegates to an external [DependencyResolver] when
 * beans are not found in the Spring context.
 *
 * This enables seamless integration between Spring Kafka and external DI containers
 * like Koin, Kodein, or any custom DI solution.
 *
 * ## Lifecycle Management
 *
 * **Important:** Resolved beans are NOT registered as Spring singletons.
 * The external DI container remains the sole owner of these beans.
 *
 * This prevents:
 * - Double ownership (bean held by both Spring and external DI)
 * - Disposal order issues (Spring trying to dispose externally-owned beans)
 * - GC prevention (bean referenced by both containers)
 *
 * ## Scope Validation
 *
 * Kafka listeners are singletons. If they depend on a scoped bean (request,
 * session, etc.), that bean is captured at construction and never refreshed.
 *
 * The factory checks [DependencyResolver.isSingleton] and logs warnings for
 * non-singleton dependencies.
 *
 * ## How It Works
 *
 * When Spring needs to autowire a dependency:
 * 1. Spring first looks in its own context
 * 2. If not found, this factory queries the [DependencyResolver]
 * 3. If found, the bean is returned directly (not cached in Spring)
 * 4. External DI container manages the bean's lifecycle
 */
class FallbackBeanFactory(
  private val dependencyResolver: DependencyResolver
) : DefaultListableBeanFactory() {
  // Track which bean names were resolved externally (for containsBean checks)
  private val externallyResolvedNames: MutableSet<String> = ConcurrentHashMap.newKeySet()

  init {
    setAutowireCandidateResolver(FallbackAutowireCandidateResolver(dependencyResolver, this))
  }

  override fun <T : Any> getBean(requiredType: Class<T>): T = try {
    super.getBean(requiredType)
  } catch (e: NoSuchBeanDefinitionException) {
    resolveFromExternal(requiredType) ?: throw e
  }

  override fun <T : Any> getBean(name: String, requiredType: Class<T>): T = try {
    super.getBean(name, requiredType)
  } catch (e: NoSuchBeanDefinitionException) {
    resolveFromExternalNamed(name, requiredType) ?: throw e
  }

  override fun containsBean(name: String): Boolean =
    super.containsBean(name) || externallyResolvedNames.contains(name)

  override fun <T : Any> getBeansOfType(type: Class<T>?): MutableMap<String, T> =
    getBeansOfType(type, true, true)

  override fun <T : Any> getBeansOfType(
    type: Class<T>?,
    includeNonSingletons: Boolean,
    allowEagerInit: Boolean
  ): MutableMap<String, T> {
    if (type == null) return super.getBeansOfType(type, includeNonSingletons, allowEagerInit)

    val springBeans = super.getBeansOfType(type, includeNonSingletons, allowEagerInit)
    if (springBeans.isNotEmpty()) return springBeans

    // Fall back to external resolver
    val externalBeans = dependencyResolver.resolveAll(type.kotlin)
    if (externalBeans.isEmpty()) return springBeans

    // Validate scope
    validateSingletonScope(type.kotlin)

    // Return external beans without registering in Spring
    val result = mutableMapOf<String, T>()
    externalBeans.forEachIndexed { index, bean ->
      val beanName = "${type.simpleName.replaceFirstChar { it.lowercase() }}$index"
      externallyResolvedNames.add(beanName)
      result[beanName] = bean
    }
    logger.debug { "Resolved ${externalBeans.size} beans of type ${type.simpleName} from external DI (not cached in Spring)" }
    return result
  }

  private fun <T : Any> resolveFromExternal(requiredType: Class<T>): T? {
    val resolved = dependencyResolver.resolve(requiredType.kotlin) ?: return null

    // Validate scope
    validateSingletonScope(requiredType.kotlin)

    val beanName = requiredType.simpleName.replaceFirstChar { it.lowercase() }
    externallyResolvedNames.add(beanName)

    logger.debug { "Resolved ${requiredType.simpleName} from external DI (owned by external container)" }
    return resolved
  }

  private fun <T : Any> resolveFromExternalNamed(name: String, requiredType: Class<T>): T? {
    val resolved = dependencyResolver.resolve(requiredType.kotlin, name) ?: return null

    // Validate scope
    validateSingletonScope(requiredType.kotlin)

    externallyResolvedNames.add(name)

    logger.debug { "Resolved ${requiredType.simpleName} (name='$name') from external DI (owned by external container)" }
    return resolved
  }

  private fun validateSingletonScope(type: kotlin.reflect.KClass<*>) {
    if (!dependencyResolver.isSingleton(type)) {
      logger.warn {
        """
        |⚠️ Non-singleton dependency resolved for Kafka listener: ${type.simpleName}
        |
        |Kafka listeners are singletons - they're created once and live for the
        |application lifetime. This dependency appears to be scoped (request,
        |session, etc.), which means:
        |
        |  - The same instance will be used for ALL Kafka messages
        |  - State will be shared across threads and messages
        |  - Scoped resources won't be released properly
        |
        |Consider using a singleton or injecting a factory/provider instead.
        """.trimMargin()
      }
    }
  }

  /**
   * Custom resolver that falls back to external DI during autowiring.
   */
  private class FallbackAutowireCandidateResolver(
    private val dependencyResolver: DependencyResolver,
    private val beanFactory: FallbackBeanFactory
  ) : ContextAnnotationAutowireCandidateResolver() {
    override fun getLazyResolutionProxyIfNecessary(
      descriptor: DependencyDescriptor,
      beanName: String?
    ): Any? {
      val result = super.getLazyResolutionProxyIfNecessary(descriptor, beanName)
      if (result != null) return result

      return resolveFromExternal(descriptor)
    }

    override fun getSuggestedValue(descriptor: DependencyDescriptor): Any? {
      val result = super.getSuggestedValue(descriptor)
      if (result != null) return result

      return resolveFromExternal(descriptor)
    }

    private fun resolveFromExternal(descriptor: DependencyDescriptor): Any? {
      val dependencyType = descriptor.dependencyType

      val resolved = dependencyResolver.resolve(dependencyType.kotlin) ?: return null

      // Validate scope
      beanFactory.validateSingletonScope(dependencyType.kotlin)

      // Track the name but don't register in Spring
      val beanName = dependencyType.simpleName.replaceFirstChar { it.lowercase() }
      beanFactory.externallyResolvedNames.add(beanName)

      logger.debug { "Resolved ${dependencyType.simpleName} from external DI for autowiring (owned by external container)" }
      return resolved
    }
  }
}
