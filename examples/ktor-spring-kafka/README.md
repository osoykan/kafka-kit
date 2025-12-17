# Ktor + Spring Kafka Example

This example demonstrates how to use **Spring Kafka** with **Ktor** as the web framework and **Koin** as the primary DI container. It showcases the native Kotlin suspend function support in [Spring Kafka 3.2+](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html) using `@KafkaListener` annotations.

## Key Features

- **Ktor** as the web framework (not Spring Boot)
- **Koin** as the primary DI container
- **Spring Kafka** with `@KafkaListener` annotation support
- **Native Kotlin suspend function support** - Spring Kafka handles them automatically
- **Minimal Spring context** - only for Kafka, not for the entire app
- **Stove** for E2E testing

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Ktor Application                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
│  │   Routes    │  │    Koin DI   │  │  SpringKafkaContext   │  │
│  │  (HTTP API) │  │   (Primary)  │  │  (Kafka-only Spring)  │  │
│  └─────────────┘  └──────────────┘  └───────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│              Spring ApplicationContext (Kafka only)             │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐  │
│  │    @KafkaListener       │  │      KafkaTemplate          │  │
│  │    Consumers            │  │      (Producer)             │  │
│  │    (suspend functions)  │  │                             │  │
│  └─────────────────────────┘  └─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                       Apache Kafka                              │
└─────────────────────────────────────────────────────────────────┘
```

## Native Suspend Function Support

Spring Kafka 3.2+ natively supports Kotlin suspend functions with `@KafkaListener`:

```kotlin
@Component
open class OrderCreatedConsumer {
  
  @KafkaListener(topics = ["example.orders.created"])
  open suspend fun consume(record: ConsumerRecord<String, DomainEvent>) {
    val event = record.value() as OrderCreatedEvent
    
    // This is a suspend function - Spring Kafka handles it natively:
    // - Runs in a coroutine context
    // - AckMode automatically set to MANUAL for async handlers
    // - Acknowledges only after successful completion
    // - Properly propagates exceptions
    
    validateOrder(event)
    processOrder(event)  // Can call other suspend functions
  }
}
```

### How Spring Kafka Handles Suspend Functions

From the [Spring Kafka documentation](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html):

> Starting with version 3.2, `@KafkaListener` (and `@KafkaHandler`) methods can be specified with asynchronous return types, letting the reply be sent asynchronously. Return types include `CompletableFuture<?>`, `Mono<?>` and Kotlin `suspend` functions.
>
> The AckMode will be automatically set to MANUAL and enable out-of-order commits when async return types are detected; instead, the asynchronous completion will ack when the async operation completes.

## Project Structure

```
src/main/kotlin/io/github/osoykan/springkafka/example/
├── ExampleKtorApp.kt           # Ktor application entry point
├── api/
│   └── Routes.kt               # HTTP endpoints
├── config/
│   └── AppConfig.kt            # Configuration classes
├── consumers/
│   ├── OrderConsumer.kt        # @KafkaListener with suspend function
│   ├── PaymentConsumer.kt      # @KafkaListener with suspend function
│   └── NotificationConsumer.kt # @KafkaListener with suspend function
├── domain/
│   └── Events.kt               # Domain event classes
└── infra/
    ├── JacksonSerde.kt         # Kafka serialization
    ├── KafkaModule.kt          # Koin module for Kafka
    └── SpringKafkaContext.kt   # Minimal Spring context for Kafka
```

## SpringKafkaContext

The `SpringKafkaContext` class creates a minimal Spring `ApplicationContext` specifically for Kafka:

```kotlin
class SpringKafkaContext(private val config: AppConfig) {
  private lateinit var applicationContext: AnnotationConfigApplicationContext

  fun start() {
    applicationContext = AnnotationConfigApplicationContext().apply {
      register(KafkaConfiguration::class.java)
      beanFactory.registerSingleton("appConfig", config)
      
      // Register consumers - Spring discovers @KafkaListener methods
      register(OrderCreatedConsumer::class.java)
      register(PaymentConsumer::class.java)
      register(NotificationConsumer::class.java)
      
      refresh()  // Starts all listener containers
    }
  }

  fun stop() {
    applicationContext.close()
  }
}
```

## Running the Example

### Prerequisites

- JDK 21+
- Docker (for Kafka in tests)

### Run the Application

```bash
# From the project root
./gradlew :examples:ktor-spring-kafka:run
```

### Run Tests

```bash
# From the project root
./gradlew :examples:ktor-spring-kafka:test
```

## Configuration

See `application.yaml`:

```yaml
server:
  port: 8080
  host: "0.0.0.0"

kafka:
  bootstrap-servers: "localhost:9092"
  group-id: "ktor-spring-kafka-example"
  
  consumer:
    enabled: true
    concurrency: 4
    poll-timeout: "1s"
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check with Kafka status |
| `POST /api/test/orders/success` | Produce a valid order |
| `POST /api/test/orders/fail-validation` | Produce order that fails validation |
| `POST /api/test/payments` | Produce a payment |
| `POST /api/test/notifications` | Produce a notification |

## Comparison with kafka-flow Example

| Feature | ktor-spring-kafka | ktor-kafka-flow |
|---------|-------------------|-----------------|
| Framework | Ktor | Ktor |
| Primary DI | Koin | Koin |
| Kafka Integration | Spring Kafka (direct) | kafka-flow (wrapper) |
| Consumer Pattern | @KafkaListener annotation | Custom @KafkaTopic annotation |
| Suspend Support | Native (Spring Kafka 3.2+) | Built-in via Flows |
| Retry Logic | Manual | Built-in |
| Metrics | Manual | Built-in |
| DLT Support | Manual | Built-in |

## References

- [Spring Kafka Async Returns](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html)
- [Spring Kafka Documentation](https://docs.spring.io/spring-kafka/reference/)
- [Ktor Documentation](https://ktor.io/docs/)
- [Koin Documentation](https://insert-koin.io/)
