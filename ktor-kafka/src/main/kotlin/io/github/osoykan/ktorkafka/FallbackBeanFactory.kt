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
 * ## How It Works
 *
 * When Spring needs to autowire a dependency:
 * 1. Spring first looks in its own context
 * 2. If not found, this factory queries the [DependencyResolver]
 * 3. If the resolver can provide the bean, it's registered as a singleton
 * 4. Future requests for the same bean return the cached instance
 */
class FallbackBeanFactory(
  private val dependencyResolver: DependencyResolver
) : DefaultListableBeanFactory() {
  private val resolvedFromExternal: MutableSet<String> = ConcurrentHashMap.newKeySet()

  init {
    // Use custom autowire candidate resolver for constructor/field injection
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
    super.containsBean(name) || resolvedFromExternal.contains(name)

  override fun <T : Any> getBeansOfType(type: Class<T>?): MutableMap<String, T> =
    getBeansOfType(type, true, true)

  override fun <T : Any> getBeansOfType(
    type: Class<T>?,
    includeNonSingletons: Boolean,
    allowEagerInit: Boolean
  ): MutableMap<String, T> {
    if (type == null) return super.getBeansOfType(type, includeNonSingletons, allowEagerInit)

    val springBeans = super.getBeansOfType(type, includeNonSingletons, allowEagerInit)

    // If Spring has beans, return them
    if (springBeans.isNotEmpty()) return springBeans

    // Fall back to external resolver
    val externalBeans = dependencyResolver.resolveAll(type.kotlin)
    if (externalBeans.isEmpty()) return springBeans

    // Register and return external beans
    val result = mutableMapOf<String, T>()
    externalBeans.forEachIndexed { index, bean ->
      val beanName = "${type.simpleName.replaceFirstChar { it.lowercase() }}$index"
      registerResolvedBean(beanName, bean)
      result[beanName] = bean
    }
    logger.debug { "Resolved ${externalBeans.size} beans of type ${type.simpleName} from external DI" }
    return result
  }

  private fun <T : Any> resolveFromExternal(requiredType: Class<T>): T? {
    val resolved = dependencyResolver.resolve(requiredType.kotlin)

    if (resolved != null) {
      val beanName = requiredType.simpleName.replaceFirstChar { it.lowercase() }
      registerResolvedBean(beanName, resolved)
      logger.debug { "Resolved ${requiredType.simpleName} from external DI container" }
    }

    return resolved
  }

  private fun <T : Any> resolveFromExternalNamed(name: String, requiredType: Class<T>): T? {
    val resolved = dependencyResolver.resolve(requiredType.kotlin, name)

    if (resolved != null) {
      registerResolvedBean(name, resolved)
      logger.debug { "Resolved ${requiredType.simpleName} (name='$name') from external DI container" }
    }

    return resolved
  }

  private fun registerResolvedBean(name: String, instance: Any) {
    if (!containsBeanDefinition(name) && !containsSingleton(name)) {
      registerSingleton(name, instance)
      resolvedFromExternal.add(name)
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
      // First try Spring's normal resolution
      val result = super.getLazyResolutionProxyIfNecessary(descriptor, beanName)
      if (result != null) return result

      // Fall back to external DI
      return resolveFromExternal(descriptor)
    }

    override fun getSuggestedValue(descriptor: DependencyDescriptor): Any? {
      // First try Spring's normal resolution
      val result = super.getSuggestedValue(descriptor)
      if (result != null) return result

      // Fall back to external DI
      return resolveFromExternal(descriptor)
    }

    private fun resolveFromExternal(descriptor: DependencyDescriptor): Any? {
      val dependencyType = descriptor.dependencyType

      // Try to resolve from external DI
      val resolved = dependencyResolver.resolve(dependencyType.kotlin)
      if (resolved != null) {
        // Register in Spring context for future use
        val beanName = dependencyType.simpleName.replaceFirstChar { it.lowercase() }
        if (!beanFactory.containsSingleton(beanName)) {
          beanFactory.registerSingleton(beanName, resolved)
          logger.debug { "Resolved ${dependencyType.simpleName} from external DI and registered in Spring" }
        }
        return resolved
      }

      return null
    }
  }
}
