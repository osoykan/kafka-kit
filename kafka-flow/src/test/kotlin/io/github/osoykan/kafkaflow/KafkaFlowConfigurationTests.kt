package io.github.osoykan.kafkaflow

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class KafkaFlowConfigurationTests :
  FunSpec({

    test("TopicConfig secondary constructor should correctly map legacy fields") {
      val config = TopicConfig(
        name = "legacy.topic",
        maxRetries = 10,
        retryBackoff = 5.seconds
      )

      config.topics shouldBe listOf("legacy.topic")
      config.maxInMemoryRetries shouldBe 10
      config.backoffMs shouldBe 5000L
    }

    test("RetryPolicy should transition from Fixed to Exponential backoff when overridden") {
      val base = RetryPolicy(
        inMemoryBackoff = BackoffStrategy.Fixed(1.seconds)
      )

      // Override with exponential parameters
      val updated = base.updateFrom(
        backoffMultiplier = 3.0
      )

      val strategy = updated.inMemoryBackoff.shouldBeInstanceOf<BackoffStrategy.Exponential>()
      strategy.multiplier shouldBe 3.0
      strategy.initialDelay shouldBe 100.milliseconds // Default from Exponential class
    }

    test("TopicConfig.mergeWith should overlay fields correctly") {
      val base = TopicConfig(
        topics = listOf("base.t"),
        concurrency = 1
      )
      val override = TopicConfig(
        concurrency = 5,
        maxInMemoryRetries = 3
      )

      val merged = base.mergeWith(override)

      merged.topics shouldBe listOf("base.t") // Unchanged
      merged.concurrency shouldBe 5 // Overridden
      merged.maxInMemoryRetries shouldBe 3 // Added
    }

    test("BackpressureConfig should reject pauseThreshold outside 0-1 range") {
      shouldThrow<IllegalArgumentException> {
        BackpressureConfig(pauseThreshold = 1.5)
      }
      shouldThrow<IllegalArgumentException> {
        BackpressureConfig(pauseThreshold = -0.1)
      }
    }

    test("BackpressureConfig should reject resumeThreshold >= pauseThreshold") {
      shouldThrow<IllegalArgumentException> {
        BackpressureConfig(pauseThreshold = 0.5, resumeThreshold = 0.5)
      }
      shouldThrow<IllegalArgumentException> {
        BackpressureConfig(pauseThreshold = 0.5, resumeThreshold = 0.6)
      }
    }

    test("TopicConfig.displayName should return single topic or comma-separated list") {
      val single = TopicConfig(name = "my.topic")
      single.displayName shouldBe "my.topic"

      val multi = TopicConfig(topics = listOf("t1", "t2", "t3"))
      multi.displayName shouldBe "t1,t2,t3"
    }

    test("TopicConfig.name should throw for multi-topic config") {
      val config = TopicConfig(topics = listOf("t1", "t2"))
      val ex = shouldThrow<IllegalStateException> { config.name }
      ex.message shouldContain "Multiple topics"
    }

    test("RetryPolicy predefined constants should have expected values") {
      RetryPolicy.NO_RETRY.maxInMemoryRetries shouldBe 0
      RetryPolicy.NO_RETRY.maxRetryTopicAttempts shouldBe 0

      RetryPolicy.AGGRESSIVE.maxInMemoryRetries shouldBe 5
      RetryPolicy.AGGRESSIVE.maxRetryTopicAttempts shouldBe 5

      RetryPolicy.TIME_LIMITED.maxRetryDuration shouldBe 5.seconds * 60
      RetryPolicy.TIME_LIMITED.maxMessageAge shouldBe 10.seconds * 60
    }

    test("BackoffStrategy.None should always return zero delay") {
      BackoffStrategy.None.delayFor(0) shouldBe Duration.ZERO
      BackoffStrategy.None.delayFor(10) shouldBe Duration.ZERO
      BackoffStrategy.None.delayFor(100) shouldBe Duration.ZERO
    }
  })
