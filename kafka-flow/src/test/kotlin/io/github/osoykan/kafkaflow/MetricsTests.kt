package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.milliseconds

class MetricsTests :
  FunSpec({

    test("NoOpMetrics should not throw on any method call") {
      NoOpMetrics.recordConsumed("topic", "consumer", 0)
      NoOpMetrics.recordProcessingSuccess("topic", "consumer", 100.milliseconds)
      NoOpMetrics.recordProcessingFailure("topic", "consumer", RuntimeException("error"))
      NoOpMetrics.recordInMemoryRetry("topic", "consumer", 1)
      NoOpMetrics.recordSentToRetryTopic("topic", "retry-topic", 1)
      NoOpMetrics.recordSentToDlt("topic", "dlt-topic", 5)
      NoOpMetrics.recordExpired("topic", "consumer", "TTL exceeded")
      NoOpMetrics.recordConsumerStarted("consumer", listOf("topic1", "topic2"))
      NoOpMetrics.recordConsumerStopped("consumer")
      // If we get here, all methods work without throwing
    }

    test("LoggingMetrics should not throw on any method call") {
      val metrics = LoggingMetrics()

      metrics.recordConsumed("topic", "consumer", 0)
      metrics.recordProcessingSuccess("topic", "consumer", 100.milliseconds)
      metrics.recordProcessingFailure("topic", "consumer", RuntimeException("error"))
      metrics.recordInMemoryRetry("topic", "consumer", 1)
      metrics.recordSentToRetryTopic("topic", "retry-topic", 1)
      metrics.recordSentToDlt("topic", "dlt-topic", 5)
      metrics.recordExpired("topic", "consumer", "TTL exceeded")
      metrics.recordConsumerStarted("consumer", listOf("topic1", "topic2"))
      metrics.recordConsumerStopped("consumer")
    }

    test("CompositeMetrics should delegate to all implementations") {
      var delegate1Called = 0
      var delegate2Called = 0

      val delegate1 = object : KafkaFlowMetrics {
        override fun recordConsumed(topic: String, consumer: String, partition: Int) {
          delegate1Called++
        }

        override fun recordProcessingSuccess(topic: String, consumer: String, duration: kotlin.time.Duration) {
          delegate1Called++
        }

        override fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable) {
          delegate1Called++
        }

        override fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int) {
          delegate1Called++
        }

        override fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int) {
          delegate1Called++
        }

        override fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int) {
          delegate1Called++
        }

        override fun recordExpired(topic: String, consumer: String, reason: String) {
          delegate1Called++
        }

        override fun recordConsumerStarted(consumer: String, topics: List<String>) {
          delegate1Called++
        }

        override fun recordConsumerStopped(consumer: String) {
          delegate1Called++
        }
      }

      val delegate2 = object : KafkaFlowMetrics {
        override fun recordConsumed(topic: String, consumer: String, partition: Int) {
          delegate2Called++
        }

        override fun recordProcessingSuccess(topic: String, consumer: String, duration: kotlin.time.Duration) {
          delegate2Called++
        }

        override fun recordProcessingFailure(topic: String, consumer: String, exception: Throwable) {
          delegate2Called++
        }

        override fun recordInMemoryRetry(topic: String, consumer: String, attempt: Int) {
          delegate2Called++
        }

        override fun recordSentToRetryTopic(topic: String, retryTopic: String, attempt: Int) {
          delegate2Called++
        }

        override fun recordSentToDlt(topic: String, dltTopic: String, totalAttempts: Int) {
          delegate2Called++
        }

        override fun recordExpired(topic: String, consumer: String, reason: String) {
          delegate2Called++
        }

        override fun recordConsumerStarted(consumer: String, topics: List<String>) {
          delegate2Called++
        }

        override fun recordConsumerStopped(consumer: String) {
          delegate2Called++
        }
      }

      val composite = CompositeMetrics(delegate1, delegate2)

      composite.recordConsumed("t", "c", 0)
      composite.recordProcessingSuccess("t", "c", 100.milliseconds)
      composite.recordProcessingFailure("t", "c", RuntimeException())
      composite.recordInMemoryRetry("t", "c", 1)
      composite.recordSentToRetryTopic("t", "rt", 1)
      composite.recordSentToDlt("t", "dlt", 5)
      composite.recordExpired("t", "c", "reason")
      composite.recordConsumerStarted("c", listOf("t"))
      composite.recordConsumerStopped("c")

      delegate1Called shouldBe 9
      delegate2Called shouldBe 9
    }
  })
