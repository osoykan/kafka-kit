# Ktor + Spring Kafka Example

This example demonstrates how to use **Spring Kafka** with **Ktor** using a custom Ktor plugin. It showcases the native Kotlin suspend function support in [Spring Kafka 3.2+](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html).

## Key Features

- **Ktor Plugin** - `install(SpringKafka) { ... }` for idiomatic configuration
- **Auto-discovery** - Consumers with `@KafkaListener` are found via component scanning
- **Native suspend support** - Spring Kafka handles suspend functions automatically
- **Lifecycle management** - Starts/stops with Ktor application

## Usage

```kotlin
fun Application.module() {
  // Install Spring Kafka plugin
  install(SpringKafka) {
    bootstrapServers = "localhost:9092"
    groupId = "my-consumer-group"
    
    // Auto-discover consumers in these packages
    consumerPackages("com.example.consumers")
    
    consumer {
      concurrency = 4
      pollTimeout = 1.seconds
    }
    
    producer {
      acks = "all"
      retries = 3
    }
  }
  
  // Use KafkaTemplate in routes
  routing {
    post("/send") {
      val template = application.kafkaTemplate<String, MyEvent>()
      template.send("my-topic", "key", event).await()
    }
  }
}
```

## Creating Consumers

Just add `@Component` and `@KafkaListener` annotations - they're auto-discovered:

```kotlin
@Component
open class OrderConsumer {
  
  @KafkaListener(topics = ["orders"])
  open suspend fun consume(record: ConsumerRecord<String, DomainEvent>) {
    val event = record.value() as OrderCreatedEvent
    
    // This is a suspend function - Spring Kafka handles it natively:
    // - Runs in a coroutine context
    // - Acknowledges only after successful completion
    
    processOrder(event)
  }
  
  private suspend fun processOrder(event: OrderCreatedEvent) {
    // Can call other suspend functions, use async I/O, etc.
  }
}
```

## Plugin Configuration Options

```kotlin
install(SpringKafka) {
  // Required
  bootstrapServers = "localhost:9092"
  groupId = "my-group"
  
  // Package scanning
  consumerPackages("com.example.consumers", "com.example.handlers")
  
  // Serialization (defaults to Jackson)
  keySerializer = StringSerializer::class
  valueSerializer = JacksonSerializer::class
  keyDeserializer = StringDeserializer::class
  valueDeserializer = JacksonDeserializer::class
  
  // Consumer settings
  consumer {
    concurrency = 4
    pollTimeout = 1.seconds
    autoOffsetReset = "earliest"
    enableAutoCommit = false
    maxPollRecords = 500
  }
  
  // Producer settings
  producer {
    acks = "all"
    retries = 3
    compression = "lz4"
    idempotence = true
  }
  
  // Additional properties
  consumerProperty("session.timeout.ms", 30000)
  producerProperty("batch.size", 16384)
}
```

## Extension Functions

```kotlin
// Get KafkaTemplate
val template = application.kafkaTemplate<String, MyEvent>()

// Check if Kafka is running
val running = application.isSpringKafkaRunning()
```

## How Suspend Functions Work

Per [Spring Kafka 3.2+ documentation](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html):

> `@KafkaListener` methods can be specified with asynchronous return types, including Kotlin `suspend` functions.
> 
> The AckMode will be automatically set to MANUAL when async return types are detected; the asynchronous completion will ack when the async operation completes.

The plugin creates a minimal Spring `ApplicationContext` that:
1. Enables `@EnableKafka` for annotation processing
2. Uses `@ComponentScan` to find `@Component` consumers
3. Spring handles the suspend function → coroutine bridge automatically

## Project Structure

```
src/main/kotlin/.../
├── ExampleKtorApp.kt           # Ktor app with SpringKafka plugin
├── api/Routes.kt               # HTTP endpoints using kafkaTemplate()
├── consumers/
│   ├── OrderConsumer.kt        # @Component + @KafkaListener suspend
│   ├── PaymentConsumer.kt      # @Component + @KafkaListener suspend  
│   └── NotificationConsumer.kt # @Component + @KafkaListener suspend
├── domain/Events.kt            # Event classes
└── infra/
    ├── JacksonSerde.kt         # Kafka serialization
    └── SpringKafkaPlugin.kt    # The Ktor plugin
```

## Running

```bash
# Run application
./gradlew :examples:ktor-spring-kafka:run

# Run tests
./gradlew :examples:ktor-spring-kafka:test
```

## Dependencies

The plugin requires:
- `spring-kafka` - Spring Kafka core
- `kotlinx-coroutines-reactor` - Required for suspend function support

## References

- [Spring Kafka Async Returns](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html)
- [Spring Kafka Documentation](https://docs.spring.io/spring-kafka/reference/)
- [Ktor Plugins](https://ktor.io/docs/server-create-a-plugin.html)
