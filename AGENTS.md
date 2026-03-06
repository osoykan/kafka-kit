# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

kafka-kit is a Kotlin Kafka toolkit with two library modules built on Spring Kafka (no Spring Boot required):

- **kafka-flow** (`io.github.osoykan.kafkaflow`) — Flow-based consumer/producer with automatic retry, DLT, metrics, backpressure, and ordered commits
- **ktor-kafka** (`io.github.osoykan.ktorkafka`) — Ktor plugin for Spring Kafka with suspend listener support and external DI bridging (e.g., Koin)

Both target JDK 21+ (virtual threads enabled by default) and Kotlin 2.0+.

## Build Commands

```bash
# Build everything
./gradlew build

# Run all tests
./gradlew test

# Run tests for a specific module
./gradlew :kafka-flow:test
./gradlew :ktor-kafka:test

# Run a single test class
./gradlew :kafka-flow:test --tests "io.github.osoykan.kafkaflow.ProduceConsumeIntegrationTests"

# Run a single test method
./gradlew :kafka-flow:test --tests "io.github.osoykan.kafkaflow.ProduceConsumeIntegrationTests.should consume messages"

# Check formatting (ktlint via spotless)
./gradlew spotlessCheck

# Auto-fix formatting
./gradlew spotlessApply

# Run examples
./gradlew :examples:ktor-spring-kafka:run
./gradlew :examples:ktor-kafka-flow:run
```

## Testing

- **Framework:** Kotest with JUnit5 platform runner
- **Kafka brokers:** Dual-mode — EmbeddedKafkaBroker (local/TDD) or Testcontainers (CI). Selection is automatic: `CI=true` env var triggers Testcontainers, otherwise EmbeddedKafka. Override with `KAFKA_TEST_MODE=testcontainers`.
- **Shared broker:** A single Kafka instance is started via `ProjectConfig` (Kotest lifecycle) and shared across all tests in a module.
- **Test support:** `kafka-flow/src/test/.../support/` contains `KafkaBroker`, `SharedKafka`, `TestHelpers` with factory methods for creating test consumers/producers.

## Code Style

- **Formatter:** ktlint via Spotless plugin, configured in root `build.gradle.kts`
- **Indent:** 2 spaces, max line length 140 (240 in tests)
- **Kotlin warnings as errors** (`allWarningsAsErrors = true`)
- **Wildcard imports allowed** (star import threshold is 2)
- **No trailing commas**
- **EditorConfig:** `.editorconfig` at repo root controls ktlint rules

## Module Structure

```
kafka-kit/
├── kafka-flow/          # Core library: Flow-based Kafka consumer/producer
├── ktor-kafka/          # Ktor plugin wrapping Spring Kafka
├── examples/
│   ├── shared/          # Shared utilities for examples (threading)
│   ├── ktor-kafka-flow/ # Example using kafka-flow with Ktor + Koin
│   └── ktor-spring-kafka/ # Example using ktor-kafka plugin
├── build.gradle.kts     # Root build: spotless, test-logger, allWarningsAsErrors
├── gradle/libs.versions.toml  # Version catalog
└── settings.gradle.kts  # Includes all modules, TYPESAFE_PROJECT_ACCESSORS
```

## Architecture

### kafka-flow pipeline

The record processing pipeline follows this chain:

1. **`KafkaFlowFactory`** — Entry point. Creates Spring Kafka factories internally, produces `ConsumerEngine` and `KafkaTemplate`.
2. **`ConsumerEngine`** — Lifecycle manager. Discovers consumers, creates supervisors, handles start/stop.
3. **`ConsumerSupervisorFactory` → `ConsumerSupervisor`** — One supervisor per consumer. `ConsumerAutoAckSupervisor` and `ConsumerManualAckSupervisor` extend `AbstractConsumerSupervisor`.
4. **`FlowKafkaConsumer`** — Wraps `SpringKafkaPoller` to produce `Flow<AckableRecord>`. Each topic gets its own flow.
5. **`SpringKafkaPoller`** (`poller/`) — Creates Spring Kafka `ConcurrentMessageListenerContainer`, uses `AcknowledgingListenerFactory` to bridge Spring listeners into a coroutine channel, feeds records through `BackpressureController`.
6. **`AbstractConsumerSupervisor.launchConsumer()`** — Collects the flow with `flatMapMerge(concurrency)` for parallel processing.
7. **`RetryableProcessor`** (`ErrorHandling.kt`) — Handles the full retry pipeline: TTL check → exception classification → in-memory retry with backoff → retry topic → DLT.
8. **`OrderedCommitter`** — Ensures offsets commit in order despite concurrent processing. Tracks completed offsets per partition and only commits contiguous sequences.
9. **`BackpressureController`** — Monitors buffer fill level and pauses/resumes the Spring Kafka container.

### Topic configuration resolution

`TopicResolver` (default: `DefaultTopicResolver`) resolves consumer config by merging:
1. `@KafkaTopic` annotation on the consumer class (base defaults)
2. Manual `TopicConfig` overrides passed via `topicConfigs` map (field-level merge)

### Consumer types

Consumers implement either `ConsumerAutoAck<K, V>` (just `suspend fun consume(record)`) or `ConsumerManualAck<K, V>` (with explicit `Acknowledgment` handle). The `@KafkaTopic` annotation configures topics, retry policy, concurrency, and exception classification per consumer.

### ktor-kafka plugin

`SpringKafka` is a Ktor `ApplicationPlugin` that:
- Creates a `AnnotationConfigApplicationContext` with `FallbackBeanFactory`
- `FallbackBeanFactory` delegates unresolved beans to an external `DependencyResolver` (e.g., Koin)
- Registers consumer/producer factories, `KafkaTemplate`, and `ConcurrentKafkaListenerContainerFactory` as Spring beans
- Enables `@KafkaListener` annotation processing via `@EnableKafka`
- Supports named factories for multi-cluster setups

## Key Dependencies

- Spring Kafka (core, not Boot) — consumer/producer infrastructure
- Kotlin Coroutines (core, jdk8, reactor) — async processing
- kotlin-logging — structured logging
- Kotest — test framework
- Testcontainers — integration test Kafka broker
- Ktor — server framework (ktor-kafka module + examples)
- Koin — DI framework (examples)
- Stove — E2E testing framework (examples)
- Jackson 3 — JSON serialization (examples)
