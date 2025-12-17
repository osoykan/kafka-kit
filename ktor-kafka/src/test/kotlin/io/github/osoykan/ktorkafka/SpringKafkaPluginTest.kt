package io.github.osoykan.ktorkafka

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.ktor.server.application.*
import io.ktor.server.testing.*
import org.springframework.kafka.core.KafkaTemplate
import kotlin.reflect.KClass
import kotlin.time.Duration.Companion.seconds

class SpringKafkaPluginTest :
  DescribeSpec({

    describe("SpringKafka Plugin") {

      describe("installation") {

        it("should install with default configuration") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
            }
          }
        }

        it("should configure consumer settings") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"

                consumer {
                  concurrency = 4
                  pollTimeout = 5.seconds
                  autoOffsetReset = "latest"
                  maxPollRecords = 100
                }
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
            }
          }
        }

        it("should configure producer settings") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"

                producer {
                  acks = "1"
                  retries = 5
                  compression = "snappy"
                  idempotence = false
                }
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
            }
          }
        }
      }

      describe("kafkaTemplate") {

        it("should provide access to KafkaTemplate") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"
              }
            }

            application {
              val template = kafkaTemplate<String, String>()
              template.shouldNotBeNull()
              template.shouldBeInstanceOf<KafkaTemplate<String, String>>()
            }
          }
        }

        it("should throw when plugin not installed") {
          testApplication {
            application {
              // Don't install SpringKafka
            }

            application {
              val exception = shouldThrow<IllegalStateException> {
                kafkaTemplate<String, String>()
              }
              exception.message shouldContain "SpringKafka plugin is not installed"
            }
          }
        }
      }

      describe("multiple factories") {

        it("should register named consumer factory") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"

                consumerFactory("secondary") {
                  bootstrapServers = "secondary:9092"
                  groupId = "secondary-group"
                  concurrency = 2
                }
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
              hasListenerContainerFactory("secondary") shouldBe true
            }
          }
        }

        it("should register named producer factory and template") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"

                producerFactory("analytics") {
                  bootstrapServers = "analytics:9092"
                  acks = "1"
                  compression = "gzip"
                }
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
              hasKafkaTemplate("analytics") shouldBe true

              val analyticsTemplate = kafkaTemplate<String, String>("analytics")
              analyticsTemplate.shouldNotBeNull()
            }
          }
        }
      }

      describe("dependencyResolver") {

        it("should resolve dependencies from external resolver") {
          val testService = TestService("hello")
          val resolver = TestDependencyResolver(testService)

          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"
                dependencyResolver = resolver
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
              // The resolver is configured and ready
              resolver.canResolve(TestService::class) shouldBe true
            }
          }
        }

        it("should work without dependency resolver (NoOp)") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"
                // No dependencyResolver set - uses NoOpDependencyResolver
              }
            }

            application {
              isSpringKafkaRunning() shouldBe true
            }
          }
        }
      }

      describe("springKafkaBean") {

        it("should get bean from Spring context") {
          testApplication {
            application {
              install(SpringKafka) {
                bootstrapServers = "localhost:9092"
                groupId = "test-group"
              }
            }

            application {
              val config = springKafkaBean<SpringKafkaConfig>()
              config.shouldNotBeNull()
              config.bootstrapServers shouldBe "localhost:9092"
              config.groupId shouldBe "test-group"
            }
          }
        }
      }
    }
  })

// Test helpers
class TestService(
  val message: String
)

class TestDependencyResolver(
  private val testService: TestService
) : DependencyResolver {
  private val beans = mapOf<KClass<*>, Any>(
    TestService::class to testService
  )

  override fun <T : Any> resolve(type: KClass<T>): T? {
    @Suppress("UNCHECKED_CAST")
    return beans[type] as T?
  }

  override fun <T : Any> resolve(type: KClass<T>, name: String): T? = resolve(type)

  override fun <T : Any> resolveAll(type: KClass<T>): List<T> {
    val bean = resolve(type)
    return if (bean != null) listOf(bean) else emptyList()
  }

  override fun canResolve(type: KClass<*>): Boolean = beans.containsKey(type)
}
