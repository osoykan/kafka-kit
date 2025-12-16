package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.annotation.EnabledIf
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

/**
 * Performance tests for kafka-flow.
 *
 * These tests are disabled by default and only run when PERFORMANCE_TESTS=true.
 *
 * Run with: PERFORMANCE_TESTS=true ./gradlew test --tests "*.PerformanceTests"
 */
@EnabledIf(PerformanceTestsEnabled::class)
class PerformanceTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("throughput: 1000 messages") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val config = TestHelpers.testListenerConfig(concurrency = 4)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic, concurrency = 4)

      val messageCount = 1000
      val receivedCount = AtomicInteger(0)

      val consumeJob = async {
        consumer.consume(topicConfig).take(messageCount).collect {
          receivedCount.incrementAndGet()
        }
      }

      delay(2.seconds)

      // Measure send time
      val sendDuration = measureTime {
        (1..messageCount).forEach { i ->
          producer.send(topic, "key-$i", "value-$i")
        }
      }

      // Wait for consumption
      val consumeDuration = measureTime {
        consumeJob.await()
      }

      consumer.stop()

      val totalDuration = sendDuration + consumeDuration
      val throughput = messageCount / totalDuration.inWholeSeconds.coerceAtLeast(1)

      println(
        """
        |
        |=== Throughput Test Results ===
        |Messages:     $messageCount
        |Send time:    $sendDuration
        |Consume time: $consumeDuration
        |Total time:   $totalDuration
        |Throughput:   $throughput msg/sec
        |Received:     ${receivedCount.get()}
        |===============================
        """.trimMargin()
      )

      receivedCount.get() shouldBe messageCount
    }

    test("throughput: 10000 messages with batching") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 8)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val config = TestHelpers.testListenerConfig(concurrency = 8)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic, concurrency = 8)

      val messageCount = 10_000
      val receivedCount = AtomicInteger(0)

      val consumeJob = async {
        consumer.consume(topicConfig).take(messageCount).collect {
          receivedCount.incrementAndGet()
        }
      }

      delay(3.seconds)

      // Send in batches
      val sendDuration = measureTime {
        (1..messageCount).chunked(500).forEach { batch ->
          batch.forEach { i ->
            producer.send(topic, "key-$i", "value-$i")
          }
        }
      }

      val consumeDuration = measureTime {
        consumeJob.await()
      }

      consumer.stop()

      val totalDuration = sendDuration + consumeDuration
      val throughput = messageCount / totalDuration.inWholeSeconds.coerceAtLeast(1)

      println(
        """
        |
        |=== High Throughput Test Results ===
        |Messages:     $messageCount
        |Send time:    $sendDuration
        |Consume time: $consumeDuration
        |Total time:   $totalDuration
        |Throughput:   $throughput msg/sec
        |Received:     ${receivedCount.get()}
        |====================================
        """.trimMargin()
      )

      receivedCount.get() shouldBe messageCount
    }

    test("latency: measure end-to-end latency") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val consumer = FlowKafkaConsumer(consumerFactory, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val messageCount = 100
      val latencies = mutableListOf<Long>()
      val receivedCount = AtomicInteger(0)

      val consumeJob = async {
        consumer.consume(topicConfig).take(messageCount).collect { record ->
          val sentTime = record.headerAsString("sent-time")?.toLongOrNull()
          if (sentTime != null) {
            latencies.add(System.currentTimeMillis() - sentTime)
          }
          receivedCount.incrementAndGet()
        }
      }

      delay(2.seconds)

      // Send messages with timestamp header
      (1..messageCount).forEach { i ->
        val headers = mapOf("sent-time" to System.currentTimeMillis().toString().toByteArray())
        producer.send(topic, "key-$i", "value-$i", headers)
        delay(10.milliseconds) // Small delay between messages for accurate latency
      }

      consumeJob.await()
      consumer.stop()

      val avgLatency = latencies.average()
      val minLatency = latencies.minOrNull() ?: 0
      val maxLatency = latencies.maxOrNull() ?: 0
      val p50 = latencies.sorted().getOrNull(latencies.size / 2) ?: 0
      val p95 = latencies.sorted().getOrNull((latencies.size * 0.95).toInt()) ?: 0
      val p99 = latencies.sorted().getOrNull((latencies.size * 0.99).toInt()) ?: 0

      println(
        """
        |
        |=== Latency Test Results ===
        |Messages:  $messageCount
        |Avg:       ${avgLatency.toLong()}ms
        |Min:       ${minLatency}ms
        |Max:       ${maxLatency}ms
        |P50:       ${p50}ms
        |P95:       ${p95}ms
        |P99:       ${p99}ms
        |============================
        """.trimMargin()
      )

      receivedCount.get() shouldBe messageCount
      avgLatency.toLong() shouldBeLessThan 1000L // Average latency should be under 1 second
    }

    test("retry performance: measure retry overhead") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, retryTopic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val policy = RetryPolicy(
        maxInMemoryRetries = 3,
        inMemoryBackoff = BackoffStrategy.Fixed(10.milliseconds)
      )
      val processor = RetryableProcessor(kafkaTemplate, policy)

      val messageCount = 100
      var successCount = 0
      var retryCount = 0

      val totalTime = measureTime {
        repeat(messageCount) { i ->
          val record = org.apache.kafka.clients.consumer
            .ConsumerRecord(topic, 0, i.toLong(), "key-$i", "value-$i")
          var attempts = 0

          val result = processor.process(record) {
            attempts++
            if (attempts < 2) {
              retryCount++
              throw RuntimeException("Temporary failure")
            }
            "success"
          }

          if (result is ProcessingResult.Success<*>) {
            successCount++
          }
        }
      }

      val avgTimePerMessage = totalTime.inWholeMilliseconds / messageCount

      println(
        """
        |
        |=== Retry Performance Results ===
        |Messages:      $messageCount
        |Successes:     $successCount
        |Total retries: $retryCount
        |Total time:    $totalTime
        |Avg per msg:   ${avgTimePerMessage}ms
        |=================================
        """.trimMargin()
      )

      successCount shouldBe messageCount
    }

    test("concurrent consumers: measure scaling") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 8)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(kafkaTemplate)

      val messageCount = 1000
      val results = mutableMapOf<Int, Long>()

      // Test with different concurrency levels
      listOf(1, 2, 4, 8).forEach { concurrency ->
        val consumerFactory = kafka.createStringConsumerFactory("$groupId-$concurrency")
        val config = TestHelpers.testListenerConfig(concurrency = concurrency)
        val consumer = FlowKafkaConsumer(consumerFactory, config)
        val topicConfig = TopicConfig(name = topic, concurrency = concurrency)

        val receivedCount = AtomicInteger(0)

        val consumeJob = async {
          consumer.consume(topicConfig).take(messageCount).collect {
            receivedCount.incrementAndGet()
          }
        }

        delay(2.seconds)

        // Send messages
        (1..messageCount).forEach { i ->
          producer.send(topic, "key-$i", "value-$i")
        }

        val duration = measureTime {
          consumeJob.await()
        }

        consumer.stop()
        results[concurrency] = duration.inWholeMilliseconds

        // Wait for consumer group to stabilize before next test
        delay(1.seconds)
      }

      println(
        """
        |
        |=== Concurrent Consumer Results ===
        |Messages: $messageCount
        |Concurrency -> Time
        ${results.entries.joinToString("\n") { "|  ${it.key} consumers -> ${it.value}ms" }}
        |===================================
        """.trimMargin()
      )
    }
  })

/**
 * Condition to enable performance tests.
 * Only runs when PERFORMANCE_TESTS=true environment variable is set.
 */
class PerformanceTestsEnabled : io.kotest.core.annotation.Condition {
  override fun evaluate(kclass: kotlin.reflect.KClass<out io.kotest.core.spec.Spec>): Boolean =
    System.getenv("PERFORMANCE_TESTS")?.toBoolean() == true
}
