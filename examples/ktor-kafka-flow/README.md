# Ktor Kafka Flow Example

This example demonstrates the **lean consumer pattern** with `kafka-flow` in a Ktor application using Koin for dependency injection.

## Key Concepts Demonstrated

### 1. Lean Consumer Pattern

Consumers only implement business logic - all retry, DLT, metrics handling is automatic:

```kotlin
@KafkaTopic(
    name = "example.orders.created",
    retry = "example.orders.created.retry",
    dlt = "example.orders.created.dlt",
    maxInMemoryRetries = 3,
    maxRetryTopicAttempts = 2
)
class OrderCreatedConsumer : ConsumerAutoAck<String, String> {
    
    // That's it! Just business logic.
    override suspend fun consume(record: ConsumerRecord<String, String>) {
        orderService.processOrder(record.value())
    }
}
```

### 2. Per-Consumer Configuration via Annotations

All retry policies are configurable per consumer using `@KafkaTopic`:

```kotlin
@KafkaTopic(
    name = "example.payments",
    maxInMemoryRetries = 5,
    maxRetryTopicAttempts = 3,
    backoffMs = 200,
    backoffMultiplier = 2.0,
    maxRetryDurationMs = 300_000,  // 5 minutes max
    classifier = ClassifierType.ALWAYS_RETRY  // Retry all exceptions
)
class PaymentConsumer : ConsumerManualAck<String, String> { ... }
```

### 3. Exception Classification

Control which exceptions trigger retries:

- `ClassifierType.DEFAULT` - Validation errors go to DLT immediately
- `ClassifierType.ALWAYS_RETRY` - Retry all exceptions (good for external APIs)
- `ClassifierType.NEVER_RETRY` - Never retry (idempotency-critical)

### 4. Manual vs Auto Acknowledgment

- `ConsumerAutoAck` - Automatic commit on success
- `ConsumerManualAck` - You control when to acknowledge

### 5. Virtual Threads for Kafka Polling

JDK 21+ Virtual Threads are used for `consumer.poll()` by default:

```yaml
kafka:
  use-virtual-threads: true
```

## Project Structure

```
examples/ktor-kafka-flow/
├── src/main/kotlin/.../example/
│   ├── ExampleKtorApp.kt           # Main entry point
│   ├── config/AppConfig.kt         # Configuration classes
│   ├── consumers/
│   │   ├── OrderConsumer.kt        # Auto-ack consumer example
│   │   └── PaymentConsumer.kt      # Manual-ack consumer example
│   ├── infra/KafkaModule.kt        # Koin DI wiring
│   └── api/Routes.kt               # HTTP endpoints
└── src/main/resources/
    └── application.yaml            # Configuration
```

## Running the Example

### Prerequisites

1. Kafka running locally on `localhost:9092`
2. JDK 21+

### Start with Docker Compose

```bash
# Start Kafka
docker-compose up -d kafka

# Run the application
./gradlew :examples:ktor-kafka-flow:run
```

### Test Endpoints

```bash
# Health check
curl http://localhost:8080/health

# Produce a successful order
curl -X POST http://localhost:8080/api/test/orders/success

# Produce an order that fails temporarily (will be retried)
curl -X POST http://localhost:8080/api/test/orders/fail-temp

# Produce an order with validation error (goes to DLT immediately)
curl -X POST http://localhost:8080/api/test/orders/fail-validation

# Produce a payment
curl -X POST http://localhost:8080/api/test/payments
```

## What the Consumer DOESN'T Care About

| Concern | Handled By |
|---------|------------|
| In-memory retries with backoff | Supervisor → RetryableProcessor |
| Retry topic publishing | Supervisor → RetryableProcessor |
| DLT publishing | Supervisor → RetryableProcessor |
| Metrics collection | Supervisor |
| Virtual threads for poll() | FlowKafkaConsumer |
| Exception classification | RetryableProcessor |
| Alerting on failures | Metrics → Alert rules |
| DLT message handling | Separate DLT consumer |

**Consumer only implements:** `suspend fun consume(record)` - that's it!

## Handling DLT Messages

Create a separate consumer for the DLT topic:

```kotlin
@KafkaTopic(name = "example.orders.created.dlt")
class OrderCreatedDltConsumer : ConsumerAutoAck<String, String> {
    
    override suspend fun consume(record: ConsumerRecord<String, String>) {
        // Alert, log, or take corrective action
        alerting.critical("Order permanently failed", record.key())
    }
}
```
