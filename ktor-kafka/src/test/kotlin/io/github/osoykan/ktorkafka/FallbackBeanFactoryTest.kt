package io.github.osoykan.ktorkafka

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import kotlin.reflect.KClass

class FallbackBeanFactoryTest :
  DescribeSpec({

    describe("FallbackBeanFactory") {

      describe("dependency resolution fallback") {

        it("should resolve bean from external resolver when not in Spring context") {
          // Given: A resolver that provides TestRepository
          val testRepository = TestRepository("test-db")
          val resolver = MapDependencyResolver(
            TestRepository::class to testRepository
          )

          // And: A FallbackBeanFactory with the resolver
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We request TestRepository (not registered in Spring)
          val resolved = beanFactory.getBean(TestRepository::class.java)

          // Then: It should be resolved from the external resolver
          resolved shouldNotBe null
          resolved shouldBe testRepository
          resolved.connectionString shouldBe "test-db"
        }

        it("should return same instance when resolver provides singleton") {
          // Given: A resolver with a singleton service
          val service = TestService("singleton-test")
          val resolver = MapDependencyResolver(
            TestService::class to service
          )
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We resolve the same bean twice
          val first = beanFactory.getBean(TestService::class.java)
          val second = beanFactory.getBean(TestService::class.java)

          // Then: Both should be the same instance (because resolver returns same instance)
          // Note: Bean is NOT cached in Spring, resolver is called each time
          first shouldBe second
          (first === second) shouldBe true
        }

        it("should prefer Spring beans over external resolver") {
          // Given: A resolver that provides TestService
          val externalService = TestService("from-external")
          val resolver = MapDependencyResolver(
            TestService::class to externalService
          )
          val beanFactory = FallbackBeanFactory(resolver)

          // And: A Spring-registered bean with the same type
          val springService = TestService("from-spring")
          beanFactory.registerSingleton("testService", springService)

          // When: We request TestService
          val resolved = beanFactory.getBean(TestService::class.java)

          // Then: Spring bean should be preferred
          resolved shouldBe springService
          resolved.message shouldBe "from-spring"
        }

        it("should throw NoSuchBeanDefinitionException when bean not found anywhere") {
          // Given: An empty resolver
          val resolver = MapDependencyResolver()
          val beanFactory = FallbackBeanFactory(resolver)

          // When/Then: Requesting unknown bean should throw
          shouldThrow<NoSuchBeanDefinitionException> {
            beanFactory.getBean(UnknownService::class.java)
          }
        }

        it("should resolve named beans from external resolver") {
          // Given: A resolver with a named bean
          val primaryDb = TestRepository("primary-db")
          val resolver = object : DependencyResolver {
            override fun <T : Any> resolve(type: KClass<T>): T? = null

            @Suppress("UNCHECKED_CAST")
            override fun <T : Any> resolve(type: KClass<T>, name: String): T? =
              if (type == TestRepository::class && name == "primaryDb") primaryDb as T else null

            override fun <T : Any> resolveAll(type: KClass<T>): List<T> = emptyList()

            override fun canResolve(type: KClass<*>): Boolean = false
          }
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We request the named bean
          val resolved = beanFactory.getBean("primaryDb", TestRepository::class.java)

          // Then: It should be resolved
          resolved shouldBe primaryDb
          resolved.connectionString shouldBe "primary-db"
        }

        it("should track externally resolved beans") {
          // Given: A resolver with a service
          val service = TestService("tracked")
          val resolver = MapDependencyResolver(
            TestService::class to service
          )
          val beanFactory = FallbackBeanFactory(resolver)

          // Initially, bean should not exist
          beanFactory.containsBean("testService") shouldBe false

          // When: We resolve the bean
          beanFactory.getBean(TestService::class.java)

          // Then: It should be tracked
          beanFactory.containsBean("testService") shouldBe true
        }
      }

      describe("collection types (List<T> support)") {

        it("should resolve all beans of type via getBeansOfType") {
          // Given: A resolver that provides multiple handlers
          val handlers = listOf(
            TestHandler("handler1"),
            TestHandler("handler2"),
            TestHandler("handler3")
          )
          val resolver = ListDependencyResolver(handlers)
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We request all beans of type TestHandler
          val resolved = beanFactory.getBeansOfType(TestHandler::class.java)

          // Then: All handlers should be resolved
          resolved.size shouldBe 3
          resolved.values.toList() shouldBe handlers
        }

        it("should prefer Spring beans over external resolver for getBeansOfType") {
          // Given: A resolver with external handlers
          val externalHandlers = listOf(TestHandler("external"))
          val resolver = ListDependencyResolver(externalHandlers)
          val beanFactory = FallbackBeanFactory(resolver)

          // And: A Spring-registered handler
          val springHandler = TestHandler("spring")
          beanFactory.registerSingleton("springHandler", springHandler)

          // When: We request all beans of type TestHandler
          val resolved = beanFactory.getBeansOfType(TestHandler::class.java)

          // Then: Only Spring bean should be returned (Spring takes priority)
          resolved.size shouldBe 1
          resolved["springHandler"] shouldBe springHandler
        }

        it("should return empty map when no beans available") {
          // Given: Empty resolver
          val resolver = ListDependencyResolver<TestHandler>(emptyList())
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We request all beans
          val resolved = beanFactory.getBeansOfType(TestHandler::class.java)

          // Then: Should return empty map
          resolved.size shouldBe 0
        }

        it("should track externally resolved beans from getBeansOfType") {
          // Given: A resolver with handlers
          val handlers = listOf(TestHandler("tracked1"), TestHandler("tracked2"))
          val resolver = ListDependencyResolver(handlers)
          val beanFactory = FallbackBeanFactory(resolver)

          // When: We resolve via getBeansOfType
          beanFactory.getBeansOfType(TestHandler::class.java)

          // Then: Beans should be tracked (for containsBean) but NOT registered as Spring singletons
          // External DI container remains the sole owner
          beanFactory.containsBean("testHandler0") shouldBe true
          beanFactory.containsBean("testHandler1") shouldBe true
        }
      }

      describe("with NoOpDependencyResolver") {

        it("should throw when no beans available") {
          // Given: NoOp resolver (never resolves anything)
          val beanFactory = FallbackBeanFactory(NoOpDependencyResolver)

          // When/Then: Should throw for unknown beans
          shouldThrow<NoSuchBeanDefinitionException> {
            beanFactory.getBean(TestService::class.java)
          }
        }

        it("should still work for Spring-registered beans") {
          // Given: NoOp resolver
          val beanFactory = FallbackBeanFactory(NoOpDependencyResolver)

          // And: A Spring-registered bean
          val service = TestService("spring-only")
          beanFactory.registerSingleton("testService", service)

          // When/Then: Should resolve Spring bean
          val resolved = beanFactory.getBean(TestService::class.java)
          resolved shouldBe service
        }
      }
    }
  })

// Test domain classes
class TestRepository(
  val connectionString: String
)

class TestHandler(
  val name: String
)

class UnknownService

/**
 * Simple map-based dependency resolver for testing.
 */
class MapDependencyResolver(
  vararg beans: Pair<KClass<*>, Any>
) : DependencyResolver {
  private val beanMap = beans.toMap()

  override fun <T : Any> resolve(type: KClass<T>): T? {
    @Suppress("UNCHECKED_CAST")
    return beanMap[type] as T?
  }

  override fun <T : Any> resolve(type: KClass<T>, name: String): T? = resolve(type)

  override fun <T : Any> resolveAll(type: KClass<T>): List<T> {
    val bean = resolve(type)
    return if (bean != null) listOf(bean) else emptyList()
  }

  override fun canResolve(type: KClass<*>): Boolean = beanMap.containsKey(type)
}

/**
 * Dependency resolver that returns a list of beans for resolveAll().
 */
class ListDependencyResolver<T : Any>(
  private val beans: List<T>
) : DependencyResolver {
  @Suppress("UNCHECKED_CAST")
  override fun <R : Any> resolve(type: KClass<R>): R? = beans.firstOrNull() as? R

  override fun <R : Any> resolve(type: KClass<R>, name: String): R? = resolve(type)

  @Suppress("UNCHECKED_CAST")
  override fun <R : Any> resolveAll(type: KClass<R>): List<R> = beans as List<R>

  override fun canResolve(type: KClass<*>): Boolean = beans.isNotEmpty()
}
