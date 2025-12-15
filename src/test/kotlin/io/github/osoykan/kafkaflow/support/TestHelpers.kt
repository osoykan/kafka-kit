package io.github.osoykan.kafkaflow.support

import io.github.osoykan.kafkaflow.FlowKafkaConsumer
import io.github.osoykan.kafkaflow.FlowKafkaProducer
import io.github.osoykan.kafkaflow.ListenerConfig
import io.github.osoykan.kafkaflow.TopicConfig
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Test utilities for Kafka flow testing.
 */
object TestHelpers {
  /**
   * Generates a unique topic name.
   */
  fun uniqueTopicName(prefix: String = "test-topic"): String =
    "$prefix-${UUID.randomUUID()}"

  /**
   * Generates a unique group ID.
   */
  fun uniqueGroupId(prefix: String = "test-group"): String =
    "$prefix-${UUID.randomUUID()}"

  /**
   * Creates a test topic configuration.
   */
  fun testTopicConfig(
    name: String = uniqueTopicName(),
    concurrency: Int = 1,
    pollTimeout: Duration = 500.milliseconds
  ): TopicConfig = TopicConfig(
    name = name,
    concurrency = concurrency,
    pollTimeout = pollTimeout
  )

  /**
   * Creates a test listener configuration.
   */
  fun testListenerConfig(
    concurrency: Int = 1,
    pollTimeout: Duration = 500.milliseconds
  ): ListenerConfig = ListenerConfig(
    concurrency = concurrency,
    pollTimeout = pollTimeout
  )
}

/**
 * Collects a specific number of records from a flow with timeout.
 */
suspend fun <K : Any, V : Any> Flow<ConsumerRecord<K, V>>.collectWithTimeout(
  count: Int,
  timeout: Duration = 30.seconds
): List<ConsumerRecord<K, V>> = withTimeout(timeout) {
  take(count).toList()
}

/**
 * Waits for a single record with timeout.
 */
suspend fun <K : Any, V : Any> Flow<ConsumerRecord<K, V>>.awaitFirst(
  timeout: Duration = 30.seconds
): ConsumerRecord<K, V> = withTimeout(timeout) {
  first()
}

/**
 * Publishes test messages and waits for them to be consumed.
 */
suspend fun <K : Any, V : Any> publishAndConsume(
  producer: FlowKafkaProducer<K, V>,
  consumer: FlowKafkaConsumer<K, V>,
  topic: TopicConfig,
  messages: List<Pair<K, V>>,
  timeout: Duration = 30.seconds
): List<ConsumerRecord<K, V>> = coroutineScope {
  val consumeJob = async {
    consumer.consume(topic).collectWithTimeout(messages.size, timeout)
  }

  // Give consumer time to start
  delay(500)

  // Publish messages
  messages.forEach { (key, value) ->
    producer.send(topic.name, key, value)
  }

  consumeJob.await()
}

/**
 * Creates a FlowKafkaConsumer for testing.
 */
fun <K : Any, V : Any> createTestConsumer(
  consumerFactory: ConsumerFactory<K, V>,
  listenerConfig: ListenerConfig = TestHelpers.testListenerConfig()
): FlowKafkaConsumer<K, V> = FlowKafkaConsumer(
  consumerFactory = consumerFactory,
  listenerConfig = listenerConfig
)

/**
 * Creates a FlowKafkaProducer for testing.
 */
fun <K : Any, V : Any> createTestProducer(
  kafkaTemplate: KafkaTemplate<K, V>
): FlowKafkaProducer<K, V> = FlowKafkaProducer(kafkaTemplate)

/**
 * Test message generator.
 */
class TestMessageGenerator(
  private val prefix: String = "test"
) {
  private var counter = 0

  fun nextKey(): String = "$prefix-key-${counter++}"

  fun nextValue(): String = "$prefix-value-$counter"

  fun nextPair(): Pair<String, String> = nextKey() to nextValue()

  fun nextPairs(count: Int): List<Pair<String, String>> = (1..count).map { nextPair() }
}

/**
 * Assertion helpers for test records.
 */
object RecordAssertions {
  fun <K : Any, V : Any> assertRecordEquals(
    expected: Pair<K, V>,
    actual: ConsumerRecord<K, V>
  ) {
    assert(actual.key() == expected.first) {
      "Expected key ${expected.first}, but was ${actual.key()}"
    }
    assert(actual.value() == expected.second) {
      "Expected value ${expected.second}, but was ${actual.value()}"
    }
  }

  fun <K : Any, V : Any> assertRecordsMatch(
    expected: List<Pair<K, V>>,
    actual: List<ConsumerRecord<K, V>>
  ) {
    assert(expected.size == actual.size) {
      "Expected ${expected.size} records, but was ${actual.size}"
    }
    expected.zip(actual).forEach { (exp, act) ->
      assertRecordEquals(exp, act)
    }
  }

  fun <K : Any, V : Any> assertRecordsContainAll(
    expected: List<Pair<K, V>>,
    actual: List<ConsumerRecord<K, V>>
  ) {
    val actualPairs = actual.map { it.key() to it.value() }.toSet()
    expected.forEach { exp ->
      assert(exp in actualPairs) {
        "Expected record $exp not found in actual records"
      }
    }
  }
}

/**
 * Retry helper for flaky operations.
 */
suspend fun <T> retryUntil(
  maxAttempts: Int = 10,
  delayBetween: Duration = 500.milliseconds,
  condition: suspend () -> T?
): T {
  repeat(maxAttempts) { attempt ->
    val result = condition()
    if (result != null) return result
    if (attempt < maxAttempts - 1) {
      delay(delayBetween)
    }
  }
  throw AssertionError("Condition not met after $maxAttempts attempts")
}

/**
 * Waits until a condition is true.
 */
suspend fun waitUntil(
  timeout: Duration = 30.seconds,
  checkInterval: Duration = 100.milliseconds,
  condition: suspend () -> Boolean
) = withTimeout(timeout) {
  while (!condition()) {
    delay(checkInterval)
  }
}
