# Kafka Flow

> ⚠️ **Work in Progress**: This project is under active development. APIs may change.

A Kotlin-first Kafka toolkit with two complementary libraries:

| Module | Description |
|--------|-------------|
| **[kafka-flow](#kafka-flow-library)** | Flow-based consumer/producer with automatic retry, DLT, and metrics |
| **[ktor-kafka](#ktor-kafka-plugin)** | Ktor plugin for Spring Kafka with suspend listeners and DI bridging |

Both libraries are built on **Spring Kafka** but require **no Spring Boot**.

## Requirements

| Requirement | Version | Notes |
|-------------|---------|-------|
| Kotlin | 2.0+ | |
| JDK | 21+ | Virtual Threads enabled by default |
| Spring Kafka | 3.3+ | |

---

# kafka-flow Library

A Kotlin Flow-based Kafka consumer/producer library. Write **lean consumers** that focus purely on business logic—all retry, DLT, and metrics handling is automatic.

## Features

- **Lean Consumer Pattern**: Just implement `suspend fun consume(record)`, no boilerplate
- **Flow-based API**: Consume Kafka messages as Kotlin Flows with backpressure
- **Coroutine-native**: Fully suspend-based, non-blocking async publishing
- **Two-stage Retry**: In-memory retries → Retry Topic → Dead Letter Topic
- **Per-consumer Config**: All policies configurable via `@KafkaTopic` annotation
- **Exception Classification**: Retryable vs NonRetryable, skip retries for validation errors
- **TTL Support**: Expire messages by retry duration or message age
- **Metrics Interface**: Pluggable observability (`NoOp`, `Logging`, `Micrometer`)

## Installation

```kotlin
dependencies {
    implementation("io.github.osoykan:kafka-flow:0.1.0")
}
```

## Quick Start

### The Lean Consumer Pattern

Write consumers that focus only on business logic:

```kotlin
@KafkaTopic(
    name = "orders.created",
    retry = "orders.created.retry",
    dlt = "orders.created.dlt",
    maxInMemoryRetries = 3,
    maxRetryTopicAttempts = 2
)
class OrderCreatedConsumer(
    private val orderService: OrderService
) : ConsumerAutoAck<String, OrderEvent> {
    
    override suspend fun consume(record: ConsumerRecord<String, OrderEvent>) {
        orderService.processOrder(record.value())
    }
}
```

**What happens automatically:**
- ✅ In-memory retries with exponential backoff
- ✅ Send to retry topic when in-memory retries exhausted
- ✅ Send to DLT when retry topic attempts exhausted
- ✅ Exception classification (validation errors → DLT immediately)
- ✅ TTL checks (expire old messages)
- ✅ Metrics recording
- ✅ Automatic acknowledgment on success

### Multiple Topics

Listen to multiple topics with a single consumer:

```kotlin
@KafkaTopic(
    topics = ["orders.created", "orders.updated", "orders.cancelled"],
    retry = "orders.retry",
    dlt = "orders.dlt"
)
class OrderEventsConsumer : ConsumerAutoAck<String, OrderEvent> {
    override suspend fun consume(record: ConsumerRecord<String, OrderEvent>) {
        when (record.topic()) {
            "orders.created" -> handleCreated(record.value())
            "orders.updated" -> handleUpdated(record.value())
            "orders.cancelled" -> handleCancelled(record.value())
        }
    }
}
```

## Configuration

### @KafkaTopic Annotation

```kotlin
@KafkaTopic(
    // Topic Configuration
    name = "payments",                    // Main topic (or use topics = [...])
    retry = "payments.retry",             // Retry topic (auto: {name}.retry)
    dlt = "payments.dlt",                 // DLT topic (auto: {name}.dlt)
    groupId = "payment-service",          // Consumer group
    concurrency = 4,                      // Processing concurrency
    multiplePartitions = 2,               // Container-level parallelism
    
    // In-Memory Retry Configuration
    maxInMemoryRetries = 5,
    backoffMs = 100,
    backoffMultiplier = 2.0,
    maxBackoffMs = 30_000,
    
    // Retry Topic Configuration
    maxRetryTopicAttempts = 3,
    retryTopicBackoffMs = 1_000,
    retryTopicBackoffMultiplier = 2.0,
    maxRetryTopicBackoffMs = 60_000,
    
    // TTL Configuration
    maxRetryDurationMs = 300_000,         // 5 min max from first failure
    maxMessageAgeMs = 600_000,            // 10 min max from timestamp
    
    // Exception Classification
    classifier = ClassifierType.DEFAULT,
    nonRetryableExceptions = [ValidationException::class]
)
class PaymentConsumer : ConsumerAutoAck<String, PaymentEvent> {
    override suspend fun consume(record: ConsumerRecord<String, PaymentEvent>) {
        // Your business logic
    }
}
```

### Exception Classification

```kotlin
// DEFAULT: Validation errors go to DLT immediately
@KafkaTopic(name = "orders", classifier = ClassifierType.DEFAULT)

// ALWAYS_RETRY: Retry all exceptions
@KafkaTopic(name = "notifications", classifier = ClassifierType.ALWAYS_RETRY)

// NEVER_RETRY: Never retry
@KafkaTopic(name = "critical", classifier = ClassifierType.NEVER_RETRY)
```

**Default non-retryable exceptions:**
`IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `ClassCastException`, `NumberFormatException`, `UnsupportedOperationException`, `IndexOutOfBoundsException`, `NoSuchElementException`

### Manual Acknowledgment

```kotlin
@KafkaTopic(name = "payments")
class PaymentConsumer : ConsumerManualAck<String, PaymentEvent> {
    
    override suspend fun consume(
        record: ConsumerRecord<String, PaymentEvent>, 
        ack: Acknowledgment
    ) {
        paymentService.process(record.value())
        ack.acknowledge()  // Explicit commit
    }
}
```

## Retry Flow

```
Message → TTL Check → Classification → In-Memory Retries → Retry Topic → DLT
              ↓              ↓                ↓                  ↓
           Expired     NonRetryable      Exhausted          Exhausted
              ↓              ↓                ↓                  ↓
             DLT            DLT         Retry Topic            DLT
```

## Backpressure

The library implements backpressure using Spring Kafka's `pause()` and `resume()`:

```kotlin
ListenerConfig(
    backpressure = BackpressureConfig(
        enabled = true,
        pauseThreshold = 0.8,   // Pause at 80% buffer fill
        resumeThreshold = 0.3   // Resume at 30% buffer fill
    )
)
```

## Metrics

```kotlin
interface KafkaFlowMetrics {
    fun recordConsumed(topic: String, consumer: String, partition: Int)
    fun recordProcessingSuccess(topic: String, consumer: String, duration: Duration)
    fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable)
    fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int)
    fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int)
    fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int)
    fun recordExpired(topic: String, consumer: String, reason: String)
}
```

**Built-in:** `NoOpMetrics`, `LoggingMetrics`, `CompositeMetrics`

---

# ktor-kafka Plugin

A Ktor plugin for Spring Kafka integration. Use Spring Kafka's powerful `@KafkaListener` with Ktor as your main framework.

## Features

- **Ktor Plugin**: Idiomatic `install(SpringKafka) { }` configuration
- **Suspend Listeners**: Native Kotlin coroutine support via `@KafkaListener`
- **Virtual Threads**: Enabled by default for all listeners (Java 21+)
- **DI Bridging**: Inject Koin/other DI beans into Spring Kafka listeners
- **Multi-Cluster**: Named factories for different Kafka clusters
- **No Spring Boot**: Just Spring Kafka core

## Installation

```kotlin
dependencies {
    implementation("io.github.osoykan:ktor-kafka:0.1.0")
}
```

## Quick Start

```kotlin
fun main() {
    embeddedServer(Netty, port = 8080) {
        // Install Koin first (optional, for DI bridging)
        install(Koin) {
            modules(module {
                single { OrderRepository() }
                single { NotificationService() }
            })
        }
        
        // Install Spring Kafka
        install(SpringKafka) {
            bootstrapServers = "localhost:9092"
            groupId = "my-service"
            
            // Bridge Koin dependencies to Spring
            dependencyResolver = KoinDependencyResolver(getKoin())
            
            // Scan for @KafkaListener consumers
            consumerPackages("com.example.consumers")
            
            // Configure consumers
            consumer {
                concurrency = 4
                pollTimeout = 1.seconds
            }
            
            // Configure producers
            producer {
                acks = "all"
                compression = "lz4"
            }
        }
        
        configureRouting()
    }.start(wait = true)
}
```

## Suspend Listeners

Write Kafka consumers with suspend functions:

```kotlin
@Component
class OrderConsumer(
    private val orderRepository: OrderRepository,  // From Koin!
    private val eventMetrics: EventMetricsService  // From Spring!
) {
    @KafkaListener(topics = ["orders.created"])
    suspend fun consume(record: ConsumerRecord<String, OrderEvent>) {
        val order = record.value()
        orderRepository.save(order)
        eventMetrics.recordProcessed("OrderCreated")
        logger.info { "Processed order: ${order.id}" }
    }
}
```

**Key points:**
- `suspend` functions are fully supported
- Inject dependencies from both Koin and Spring
- Virtual threads handle the blocking Kafka poll

## DI Bridging

The plugin bridges external DI containers (like Koin) to Spring:

```kotlin
// 1. Implement DependencyResolver
class KoinDependencyResolver(private val koin: Koin) : DependencyResolver {
    override fun <T : Any> resolve(type: KClass<T>): T? = 
        koin.getOrNull(type)
    
    override fun <T : Any> resolve(type: KClass<T>, name: String): T? = 
        koin.getOrNull(type, named(name))
    
    override fun <T : Any> resolveAll(type: KClass<T>): List<T> = 
        koin.getAll(type)
    
    override fun canResolve(type: KClass<*>): Boolean = 
        koin.getOrNull(type) != null
}

// 2. Configure in plugin
install(SpringKafka) {
    dependencyResolver = KoinDependencyResolver(getKoin())
    consumerPackages("com.example.consumers")
}

// 3. Now Spring @KafkaListener can inject Koin beans!
@Component
class MyConsumer(
    private val koinService: MyKoinService  // Resolved from Koin
) {
    @KafkaListener(topics = ["my-topic"])
    suspend fun handle(record: ConsumerRecord<String, String>) {
        koinService.process(record.value())
    }
}
```

## Producing Messages

```kotlin
fun Application.configureRouting() {
    val kafkaTemplate = kafkaTemplate<String, OrderEvent>()
    
    routing {
        post("/orders") {
            val order = call.receive<CreateOrderRequest>()
            val event = OrderCreatedEvent(order.id, order.items)
            
            kafkaTemplate.send("orders.created", order.id, event)
            
            call.respond(HttpStatusCode.Created, order)
        }
        }
    }
```

## Multi-Cluster Support

Configure multiple Kafka clusters:

```kotlin
install(SpringKafka) {
    // Default cluster
    bootstrapServers = "cluster-a:9092"
    groupId = "my-service"
    
    // Additional consumer factory for cluster B
    consumerFactory("clusterB") {
        bootstrapServers = "cluster-b:9092"
        groupId = "my-service-cluster-b"
        concurrency = 2
    }
    
    // Additional producer for analytics
    producerFactory("analytics") {
        bootstrapServers = "analytics:9092"
        acks = "1"
        compression = "snappy"
    }
}
```

Use named factories:

```kotlin
// Consumer with named factory
@Component
class ClusterBConsumer {
    @KafkaListener(
        topics = ["events"],
        containerFactory = "clusterBKafkaListenerContainerFactory"
    )
    suspend fun handle(record: ConsumerRecord<String, Event>) { }
}

// Producer with named template
val analyticsTemplate = application.kafkaTemplate<String, AnalyticsEvent>("analytics")
```

## Error Handling

Configure custom error handlers for consumer errors:

```kotlin
install(SpringKafka) {
    bootstrapServers = "localhost:9092"
    
    consumer {
        // Custom error handler with retry and dead-letter publishing
        errorHandler = DefaultErrorHandler(
            DeadLetterPublishingRecoverer(kafkaTemplate),
            FixedBackOff(1000L, 3L)  // 1s delay, 3 retries
        )
    }
}
```

**Available error handlers:**
- `DefaultErrorHandler` - Retries with backoff, optional dead-letter recovery
- `CommonLoggingErrorHandler` - Logs errors only, no retry
- `CommonDelegatingErrorHandler` - Delegates based on exception type

Named consumer factories can also have their own error handlers:

```kotlin
consumerFactory("critical") {
    bootstrapServers = "localhost:9092"
    errorHandler = DefaultErrorHandler(
        FixedBackOff(5000L, 10L)  // More aggressive retry
    )
}
```

## Advanced Factory Customization

For advanced use cases, customize factories after creation:

```kotlin
install(SpringKafka) {
    bootstrapServers = "localhost:9092"
    
    // Customize ConsumerFactory
    customizeConsumerFactory {
        // 'this' is ConsumerFactory<Any, Any>
        addListener(object : ConsumerFactory.Listener<Any, Any> {
            override fun consumerAdded(id: String, consumer: Consumer<Any, Any>) {
                logger.info { "Consumer added: $id" }
            }
        })
    }
    
    // Customize ProducerFactory
    customizeProducerFactory {
        // 'this' is ProducerFactory<Any, Any>
        addListener(object : ProducerFactory.Listener<Any, Any> {
            override fun producerAdded(id: String, producer: Producer<Any, Any>) {
                logger.info { "Producer added: $id" }
            }
        })
    }
    
    // Customize ConcurrentKafkaListenerContainerFactory
    customizeContainerFactory {
        // 'this' is ConcurrentKafkaListenerContainerFactory<Any, Any>
        
        // Record interceptor
        setRecordInterceptor { record, consumer ->
            logger.debug { "Intercepted: ${record.topic()}" }
            record
        }
        
        // Batch listener mode
        setBatchListener(true)
        
        // Custom after-rollback processor
        setAfterRollbackProcessor(DefaultAfterRollbackProcessor())
    }
}
```

Named factories also support customizers:

```kotlin
consumerFactory("custom") {
    bootstrapServers = "localhost:9092"
    
    customizeConsumerFactory {
        // Customize this specific consumer factory
    }
    
    customizeContainerFactory {
        // Customize this specific container factory
        setRecordInterceptor { record, _ ->
            // Add tracing headers, etc.
            record
        }
    }
}

producerFactory("custom") {
    bootstrapServers = "localhost:9092"
    
    customizeProducerFactory {
        // Customize this specific producer factory
    }
}
```

## Configuration Reference

```kotlin
install(SpringKafka) {
    // Connection
    bootstrapServers = "localhost:9092"
    groupId = "my-consumer-group"
    
    // Serialization (default: String)
    keySerializer = StringSerializer::class
    valueSerializer = JsonSerializer::class
    keyDeserializer = StringDeserializer::class
    valueDeserializer = JsonDeserializer::class
    
    // DI bridging
    dependencyResolver = KoinDependencyResolver(getKoin())
    
    // Component scanning
    consumerPackages("com.example.consumers", "com.example.handlers")
    
    // Consumer settings
    consumer {
        concurrency = 4                    // Concurrent consumers
        pollTimeout = 1.seconds            // Poll timeout
        autoOffsetReset = "earliest"       // earliest, latest, none
        enableAutoCommit = false           // Manual commits (recommended)
        maxPollRecords = 500               // Max records per poll
        errorHandler = DefaultErrorHandler() // Custom error handler
    }
    
    // Producer settings
    producer {
        acks = "all"                       // 0, 1, all
        retries = 3                        // Retry count
        compression = "lz4"                // none, gzip, snappy, lz4, zstd
        idempotence = true                 // Idempotent producer
    }
    
    // Additional properties
    consumerProperty("max.poll.interval.ms", 300000)
    producerProperty("linger.ms", 5)
}
```

## Extension Functions

```kotlin
// Get KafkaTemplate
val template = application.kafkaTemplate<String, MyEvent>()
val namedTemplate = application.kafkaTemplate<String, MyEvent>("analytics")

// Get Spring beans
val service = application.springKafkaBean<MySpringService>()

// Check status
val running = application.isSpringKafkaRunning()
val hasFactory = application.hasListenerContainerFactory("clusterB")
val hasTemplate = application.hasKafkaTemplate("analytics")
```

---

# Examples

| Example | Description |
|---------|-------------|
| [`examples/ktor-kafka-flow/`](examples/ktor-kafka-flow/) | Full kafka-flow library usage with Ktor and Koin |
| [`examples/ktor-spring-kafka/`](examples/ktor-spring-kafka/) | ktor-kafka plugin with suspend listeners and DI bridging |

## Running Examples

```bash
# Start Kafka (using Docker)
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest

# Run example
./gradlew :examples:ktor-spring-kafka:run
```

---

# Testing

Both libraries support dual testing modes:

| Mode | Implementation | Use Case |
|------|----------------|----------|
| `embedded` | EmbeddedKafkaBroker | Local TDD (~5s startup) |
| `testcontainers` | ConfluentKafkaContainer | CI, production-like |

**Automatic selection:**
- `CI=true` → Testcontainers
- Otherwise → EmbeddedKafka

**Manual override:**
```bash
KAFKA_TEST_MODE=testcontainers ./gradlew test
```

---

# License

Apache License 2.0
