# Design Decisions

This document captures the key architectural and design decisions made for the Kafka Flow library.

## 1. Spring Kafka as the Foundation

**Decision**: Use Spring Kafka 4.x as the underlying Kafka client infrastructure.

**Context**: Multiple options exist for Kafka clients in Kotlin:
- Raw Kafka clients
- reactor-kafka (deprecated)
- Spring Kafka
- kotlin-kafka

**Rationale**:
- Spring Kafka provides battle-tested handling of complex Kafka concerns:
  - Consumer rebalancing
  - Offset commit management
  - Consumer lifecycle management
  - Error handling and recovery
- Spring Kafka 4.x is actively maintained and works with Kafka 3.x+
- We don't need Spring Boot - Spring Kafka can be used standalone
- The library handles edge cases that would take significant effort to implement correctly

**Consequences**:
- Dependency on Spring Kafka (and transitively, Spring Context)
- Configuration follows Spring Kafka patterns
- Benefits from Spring Kafka's ongoing maintenance and bug fixes

---

## 2. Kotlin Flow as the Consumer API

**Decision**: Expose consumed messages as `Flow<ConsumerRecord<K, V>>` instead of callbacks or reactive streams.

**Context**: Various patterns exist for async message consumption:
- Callbacks
- Reactive Streams (Reactor, RxJava)
- Kotlin Coroutines/Flow
- Blocking iteration

**Rationale**:
- Flow provides native Kotlin coroutine integration
- Built-in backpressure support via `buffer()` and `flowOn()`
- Composable with other Flow operators (map, filter, take, etc.)
- Natural fit for suspend functions in message handlers
- Simpler than Reactive Streams for Kotlin developers

**Consequences**:
- Consumers must use coroutines
- Flow collection semantics apply (cold flow, cancellation)
- `Dispatchers.IO` is used by default for non-blocking operation

---

## 3. No Rebalance Event Exposure

**Decision**: Do not expose Kafka rebalance events to consumers.

**Context**: Spring Kafka provides hooks for partition assignment/revocation events. We initially considered exposing these.

**Rationale**:
- Spring Kafka already handles rebalancing correctly internally
- Exposing rebalance events adds complexity without clear benefit
- Most applications don't need to know about rebalances
- The library should focus on what matters: **processing messages**
- If rebalance handling is needed, users can configure Spring Kafka directly

**Consequences**:
- Simpler API with only 2 consumer methods: `consume()` and `consumeWithAck()`
- Users who need custom rebalance handling must use Spring Kafka directly
- Reduced cognitive load for library users

---

## 4. In-Memory Retry Before Retry Topic

**Decision**: Implement a two-stage retry mechanism: in-memory retries first, then retry topic.

**Context**: When message processing fails, options include:
- Immediate send to DLT
- Immediate send to retry topic
- In-memory retry then retry topic
- Only in-memory retry

**Rationale**:
- Many failures are transient (network blips, temporary unavailability)
- In-memory retry with exponential backoff handles transient failures cheaply
- Retry topics add latency and infrastructure complexity
- Two-stage approach: fast recovery for transient issues, durable retry for persistent issues

**Flow**:
```
Message → In-memory retries (with backoff) → Retry Topic → DLT
```

**Consequences**:
- More configuration options (in-memory retries, retry topic attempts, backoff strategies)
- Better handling of transient failures without topic overhead
- Clear escalation path: in-memory → retry topic → DLT

---

## 5. Configurable Backoff Strategies

**Decision**: Provide sealed class hierarchy for backoff strategies.

**Context**: Different retry scenarios need different backoff behaviors.

**Rationale**:
- `Fixed`: Simple, predictable delay (good for rate-limited APIs)
- `Exponential`: Prevents thundering herd, good for transient failures
- `None`: For testing or when delay isn't needed
- Sealed class ensures exhaustive handling and type safety

**Example**:
```kotlin
BackoffStrategy.Exponential(
    initialDelay = 100.milliseconds,
    multiplier = 2.0,
    maxDelay = 30.seconds
)
// Delays: 100ms → 200ms → 400ms → 800ms → ... → 30s (capped)
```

**Consequences**:
- Clear, type-safe configuration
- Easy to add new strategies if needed
- Predictable behavior across retry attempts

---

## 6. Dispatchers.IO by Default

**Decision**: Use `Dispatchers.IO` as the default coroutine dispatcher.

**Context**: Kafka consumers perform I/O operations and may block.

**Rationale**:
- Kafka client operations involve network I/O
- Message handlers often perform database/HTTP calls
- `Dispatchers.IO` is designed for blocking I/O operations
- Prevents blocking the main thread or limited dispatcher pools
- Users can override with custom dispatcher if needed

**Consequences**:
- Good default for typical use cases
- Thread pool sized appropriately for I/O-bound work
- Configurable for special cases (e.g., CPU-bound processing)

---

## 7. Shared Test Container

**Decision**: Use a single shared Kafka container across all test specs.

**Context**: Tests require a Kafka broker. Options:
- Embedded Kafka
- Container per test
- Container per spec
- Shared container across all specs

**Rationale**:
- Kafka containers are expensive to start (~10-15 seconds each)
- Test isolation achieved via unique topic names and group IDs
- Kotest's `AbstractProjectConfig` provides clean lifecycle hooks
- Dramatically faster test execution (1.5min vs 3+ min)

**Implementation**:
```kotlin
class ProjectConfig : AbstractProjectConfig() {
    override suspend fun beforeProject() = SharedKafka.start()
    override suspend fun afterProject() = SharedKafka.stop()
}
```

**Consequences**:
- Tests run in ~1.5 minutes instead of 3+ minutes
- Tests must use unique topic names (`TestHelpers.uniqueTopicName()`)
- Tests must use unique group IDs (`TestHelpers.uniqueGroupId()`)

---

## 8. Standalone Library (No Spring Boot Required)

**Decision**: Design the library to work without Spring Boot.

**Context**: Many applications use Ktor, http4k, or other non-Spring frameworks.

**Rationale**:
- Spring Kafka can be used standalone (only needs spring-context)
- Factory functions create all necessary Spring Kafka components
- No auto-configuration or component scanning required
- Works with any DI framework or manual instantiation

**Example**:
```kotlin
// No Spring Boot needed
val config = KafkaFlowConfig(
    bootstrapServers = "localhost:9092",
    consumer = ConsumerConfig(groupId = "my-group")
)
val consumerFactory = KafkaFlowFactories.createConsumerFactory(config, ...)
val consumer = FlowKafkaConsumer(consumerFactory, config.listener)
```

**Consequences**:
- Works with Ktor, http4k, or any framework
- Slightly more verbose setup than Spring Boot auto-config
- Full control over component lifecycle

---

## 9. Type-Safe Configuration with Data Classes

**Decision**: Use Kotlin data classes for all configuration.

**Context**: Configuration can be done via:
- Properties files
- YAML
- Environment variables
- Programmatic configuration

**Rationale**:
- Data classes provide compile-time type safety
- Default values clearly documented
- IDE auto-completion for all options
- Easy to create from any config source (HOCON, YAML, etc.)
- Immutable by default

**Example**:
```kotlin
data class RetryPolicy(
    val maxInMemoryRetries: Int = 3,
    val inMemoryBackoff: BackoffStrategy = BackoffStrategy.Exponential(),
    val maxRetryTopicAttempts: Int = 3,
    ...
)
```

**Consequences**:
- Clear documentation of all configuration options
- Compile-time validation
- Easy integration with config libraries (Hoplite, Konf, etc.)

---

## 10. Processing Result as Sealed Class

**Decision**: Return `ProcessingResult<T>` from retry-enabled processing.

**Context**: Processing with retries can have multiple outcomes.

**Rationale**:
- Clear indication of what happened:
  - `Success`: Message processed successfully
  - `SentToRetryTopic`: Will be retried later
  - `SentToDlt`: Exhausted all retries
  - `Failed`: Couldn't even send to retry/DLT
- Enables pattern matching with `when` expressions
- Forces handling of all cases

**Example**:
```kotlin
when (val result = processor.process(record) { ... }) {
    is ProcessingResult.Success -> metrics.recordSuccess()
    is ProcessingResult.SentToRetryTopic -> metrics.recordRetry()
    is ProcessingResult.SentToDlt -> metrics.recordDlt()
    is ProcessingResult.Expired -> metrics.recordExpired()
    is ProcessingResult.Failed -> metrics.recordFailed()
}
```

**Consequences**:
- Explicit handling of all outcomes
- No exceptions for expected retry scenarios
- Clear metrics and logging opportunities

---

## 11. Lean Consumer Pattern

**Decision**: Provide interface-based consumer pattern where consumers only implement business logic.

**Context**: Consumers often contain boilerplate for retry handling, metrics, and error management.

**Rationale**:
- Consumers should focus solely on business logic
- All infrastructure concerns (retry, DLT, metrics) handled by supervisors
- Configuration via annotations (`@KafkaTopic`) keeps consumers clean
- Follows separation of concerns principle

**Implementation**:
```kotlin
@KafkaTopic(
    name = "orders.created",
    retry = "orders.created.retry",
    dlt = "orders.created.dlt",
    maxInMemoryRetries = 3
)
class OrderConsumer : ConsumerAutoAck<String, OrderEvent> {
    override suspend fun consume(record: ConsumerRecord<String, OrderEvent>) {
        // Just business logic - no retry/error handling needed!
        orderService.process(record.value())
    }
}
```

**Consequences**:
- Clean, focused consumer implementations
- Configuration is declarative and type-safe
- Retry logic completely abstracted away
- Easy to test consumers in isolation

---

## 12. Exception Classification

**Decision**: Classify exceptions to determine retry behavior.

**Context**: Not all exceptions should trigger retries.

**Rationale**:
- Validation errors (IllegalArgumentException, etc.) won't succeed on retry
- Transient errors (network, temporary unavailability) should be retried
- Classification allows skipping retries for known non-retryable exceptions
- Reduces unnecessary retry attempts and latency

**Categories**:
- `Retryable`: Transient errors - retry may succeed
- `NonRetryable`: Permanent errors - skip to DLT immediately

**Classifiers**:
- `DefaultExceptionClassifier`: Validation errors → NonRetryable
- `AlwaysRetryClassifier`: Retry all exceptions (for external APIs)
- `NeverRetryClassifier`: Never retry (idempotency-critical)

**Consequences**:
- Faster DLT routing for validation errors
- Configurable per consumer via annotation
- Extensible for custom exception types

---

## 13. TTL / Max Age Support

**Decision**: Allow messages to expire based on time spent in retry or age.

**Context**: Some messages become irrelevant after a certain time.

**Rationale**:
- Stale data shouldn't be processed indefinitely
- Prevents retry queue buildup
- Time-sensitive operations need bounds
- Two dimensions: retry duration and message age

**Configuration**:
```kotlin
RetryPolicy(
    maxRetryDuration = 5.minutes,  // Max time from first failure
    maxMessageAge = 10.minutes     // Max age from original timestamp
)
```

**Consequences**:
- Messages can expire before exhausting retries
- Expired messages go to DLT with expiry reason
- Clear metrics for monitoring expiry rates

---

## 14. Virtual Threads for Kafka Polling

**Decision**: Use JDK 21+ Virtual Threads for Spring Kafka's `consumer.poll()` operation.

**Context**: `consumer.poll()` is a blocking I/O operation.

**Rationale**:
- Virtual threads are ideal for blocking I/O (cheap, unlimited)
- Kafka polling is inherently blocking
- Kotlin coroutines remain on `Dispatchers.IO` for processing
- Clear separation: Virtual Threads for polling, Coroutines for processing

**Architecture**:
```
┌─────────────────────────────────────────────────────────────────┐
│  Kotlin Coroutines Layer (Dispatchers.IO)                       │
│  └─ Flow.collect { consumer.consume(record) }                   │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │ emits records
┌─────────────────────────────────────────────────────────────────┐
│  Spring Kafka Container Layer (Virtual Threads)                  │
│  └─ consumer.poll() - blocking I/O                              │
└─────────────────────────────────────────────────────────────────┘
```

**Consequences**:
- Better resource utilization for high-concurrency consumers
- Configurable via `ListenerConfig.useVirtualThreads`
- Default: enabled (opt-out if needed)

---

## 15. Non-Blocking Async Publishing

**Decision**: Use `await()` extension instead of blocking `.get()` for Kafka publishing.

**Context**: Publishing to retry/DLT topics was using blocking `Future.get()`.

**Rationale**:
- Blocking calls in coroutines waste threads
- `CompletableFuture.await()` suspends without blocking
- Consistent with coroutine-first design
- Better throughput under load

**Implementation**:
```kotlin
// Before
kafkaTemplate.send(record).get()  // Blocks thread

// After
kafkaTemplate.send(record).completable().await()  // Suspends
```

**Consequences**:
- More efficient thread utilization
- Consistent suspend-based API
- Better scalability for high-throughput scenarios

---

## 16. Metrics Interface Abstraction

**Decision**: Define a `KafkaFlowMetrics` interface for observability.

**Context**: Production systems need comprehensive metrics.

**Rationale**:
- Decouple metrics collection from implementation
- NoOp implementation for when metrics disabled
- Easy integration with Micrometer, Prometheus, etc.
- Consumers don't interact with metrics directly

**Metrics**:
- Consumer: consumed, success, failure
- Retry: in-memory retry, sent to retry topic, sent to DLT
- Lifecycle: consumer started/stopped
- TTL: expired messages

**Consequences**:
- Optional metrics (NoOp by default)
- Easy integration with existing observability stack
- Comprehensive visibility into consumer behavior
