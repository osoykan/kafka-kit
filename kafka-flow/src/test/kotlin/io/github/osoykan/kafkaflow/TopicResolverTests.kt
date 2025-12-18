package io.github.osoykan.kafkaflow

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.Duration.Companion.milliseconds

class TopicResolverTests :
  FunSpec({

    test("TopicResolver should resolve config from @KafkaTopic annotation") {
      @KafkaTopic(
        name = "test.orders",
        retry = "test.orders.retry",
        dlt = "test.orders.dlt",
        concurrency = 4,
        maxInMemoryRetries = 5,
        backoffMs = 200,
        maxRetryTopicAttempts = 3
      )
      class TestOrderConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestOrderConsumer())

      config.topic.name shouldBe "test.orders"
      config.topic.retryTopic shouldBe "test.orders.retry"
      config.topic.dltTopic shouldBe "test.orders.dlt"
      config.topic.concurrency shouldBe 4
      config.retry.maxInMemoryRetries shouldBe 5
      config.retry.maxRetryTopicAttempts shouldBe 3
    }

    test("TopicResolver should use default retry/dlt suffixes when not specified") {
      @KafkaTopic(name = "test.payments")
      class TestPaymentConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestPaymentConsumer())

      config.topic.name shouldBe "test.payments"
      config.retryTopic shouldBe "test.payments.retry"
      config.dltTopic shouldBe "test.payments.dlt"
    }

    test("TopicResolver should configure TTL settings from annotation") {
      @KafkaTopic(
        name = "test.events",
        maxRetryDurationMs = 300_000, // 5 minutes
        maxMessageAgeMs = 600_000 // 10 minutes
      )
      class TestEventConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestEventConsumer())

      config.retry.maxRetryDuration shouldNotBe null
      config.retry.maxRetryDuration?.inWholeMilliseconds shouldBe 300_000
      config.retry.maxMessageAge shouldNotBe null
      config.retry.maxMessageAge?.inWholeMilliseconds shouldBe 600_000
    }

    test("TopicResolver should configure classifier from annotation") {
      @KafkaTopic(
        name = "test.notifications",
        classifier = ClassifierType.ALWAYS_RETRY
      )
      class TestNotificationConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestNotificationConsumer())

      // AlwaysRetryClassifier should retry everything
      config.classifier.classify(IllegalArgumentException()) shouldBe ExceptionCategory.Retryable
    }

    test("TopicResolver should configure NeverRetry classifier") {
      @KafkaTopic(
        name = "test.critical",
        classifier = ClassifierType.NEVER_RETRY
      )
      class TestCriticalConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestCriticalConsumer())

      // NeverRetryClassifier should never retry
      config.classifier.classify(RuntimeException()) shouldBe ExceptionCategory.NonRetryable
    }

    test("TopicResolver should fall back to config map when no annotation") {
      class TestLegacyConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val topicConfigs = mapOf(
        "TestLegacyConsumer" to TopicConfig(
          name = "legacy.topic",
          retryTopic = "legacy.topic.retry",
          dltTopic = "legacy.topic.dlt"
        )
      )

      val resolver = DefaultTopicResolver(topicConfigs = topicConfigs)
      val config = resolver.resolve(TestLegacyConsumer())

      config.topic.name shouldBe "legacy.topic"
    }

    test("TopicResolver should configure exponential backoff from annotation") {
      @KafkaTopic(
        name = "test.backoff",
        backoffMs = 100,
        backoffMultiplier = 3.0,
        maxBackoffMs = 10_000
      )
      class TestBackoffConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val config = resolver.resolve(TestBackoffConsumer())

      val backoff = config.retry.inMemoryBackoff as BackoffStrategy.Exponential
      backoff.initialDelay shouldBe 100.milliseconds
      backoff.multiplier shouldBe 3.0
      backoff.maxDelay shouldBe 10_000.milliseconds
    }

    test("Consumer.consumerName should return simple class name by default") {
      class MySpecialConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val consumer = MySpecialConsumer()
      consumer.consumerName shouldBe "MySpecialConsumer"
    }

    test("ResolvedConsumerConfig should compute effective retry and DLT topics") {
      val config = ResolvedConsumerConfig(
        topic = TopicConfig(name = "my.topic"),
        retry = RetryPolicy(retryTopicSuffix = ".retry", dltSuffix = ".dlt"),
        classifier = DefaultExceptionClassifier(),
        consumerName = "MyConsumer"
      )

      config.retryTopic shouldBe "my.topic.retry"
      config.dltTopic shouldBe "my.topic.dlt"
    }

    test("ResolvedConsumerConfig should prefer explicit topic names over suffixes") {
      val config = ResolvedConsumerConfig(
        topic = TopicConfig(
          name = "my.topic",
          retryTopic = "custom.retry.topic",
          dltTopic = "custom.dlt.topic"
        ),
        retry = RetryPolicy(retryTopicSuffix = ".retry", dltSuffix = ".dlt"),
        classifier = DefaultExceptionClassifier(),
        consumerName = "MyConsumer"
      )

      config.retryTopic shouldBe "custom.retry.topic"
      config.dltTopic shouldBe "custom.dlt.topic"
    }

    test("ResolvedConsumerConfig should throw error when auto-generating retry topic for multiple main topics") {
      val config = ResolvedConsumerConfig(
        topic = TopicConfig(topics = listOf("topic1", "topic2")),
        retry = RetryPolicy(),
        classifier = DefaultExceptionClassifier(),
        consumerName = "MultiTopicConsumer"
      )

      val retryEx = shouldThrow<IllegalStateException> { config.retryTopic }
      retryEx.message shouldContain "listens to multiple topics"

      val dltEx = shouldThrow<IllegalStateException> { config.dltTopic }
      dltEx.message shouldContain "listens to multiple topics"
    }

    test("DefaultTopicResolver should merge manual config overrides into annotation values") {
      @KafkaTopic(
        name = "test.orders",
        concurrency = 4,
        maxInMemoryRetries = 5
      )
      class MergingConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val topicConfigs = mapOf(
        "MergingConsumer" to TopicConfig(
          concurrency = 8, // Override
          maxInMemoryRetries = 2 // Override
        )
      )

      val resolver = DefaultTopicResolver(topicConfigs = topicConfigs)
      val config = resolver.resolve(MergingConsumer())

      config.topic.name shouldBe "test.orders" // From annotation
      config.topic.concurrency shouldBe 8 // Overridden
      config.retry.maxInMemoryRetries shouldBe 2 // Overridden
    }

    test("DefaultTopicResolver should support partial merging of backoff settings") {
      @KafkaTopic(
        name = "test.backoff",
        backoffMs = 100,
        backoffMultiplier = 2.0
      )
      class BackoffMergingConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val topicConfigs = mapOf(
        "BackoffMergingConsumer" to TopicConfig(
          backoffMultiplier = 4.0 // Override only multiplier
        )
      )

      val resolver = DefaultTopicResolver(topicConfigs = topicConfigs)
      val config = resolver.resolve(BackoffMergingConsumer())

      val backoff = config.retry.inMemoryBackoff as BackoffStrategy.Exponential
      backoff.initialDelay shouldBe 100.milliseconds // From annotation
      backoff.multiplier shouldBe 4.0 // Overridden
    }

    test("DefaultTopicResolver should throw error when no topics are provided anywhere") {
      @KafkaTopic() // No name or topics
      class InvalidConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val resolver = DefaultTopicResolver()
      val ex = shouldThrow<IllegalArgumentException> {
        resolver.resolve(InvalidConsumer())
      }
      ex.message shouldContain "No topics configured"
    }

    test("DefaultTopicResolver should allow fully manual configuration for consumer without annotation") {
      class ManualOnlyConsumer : ConsumerAutoAck<String, String> {
        override suspend fun consume(record: ConsumerRecord<String, String>) {}
      }

      val topicConfigs = mapOf(
        "ManualOnlyConsumer" to TopicConfig(
          topics = listOf("manual.topic"),
          concurrency = 10
        )
      )

      val resolver = DefaultTopicResolver(topicConfigs = topicConfigs)
      val config = resolver.resolve(ManualOnlyConsumer())

      config.topic.topics shouldBe listOf("manual.topic")
      config.topic.concurrency shouldBe 10
    }

    test("ResolvedConsumerConfig should work with multiple topics if explicit retry topic is provided") {
      val config = ResolvedConsumerConfig(
        topic = TopicConfig(
          topics = listOf("t1", "topic2"),
          retryTopic = "explicit.retry"
        ),
        retry = RetryPolicy(),
        classifier = DefaultExceptionClassifier(),
        consumerName = "MultiTopicConsumer"
      )

      config.retryTopic shouldBe "explicit.retry"
    }
  })
