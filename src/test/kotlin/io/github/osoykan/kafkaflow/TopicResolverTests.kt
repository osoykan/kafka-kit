package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
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

      val resolver = TopicResolver(defaultGroupId = "test-group")
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

      val resolver = TopicResolver(defaultGroupId = "test-group")
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

      val resolver = TopicResolver()
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

      val resolver = TopicResolver()
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

      val resolver = TopicResolver()
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

      val resolver = TopicResolver(topicConfigs = topicConfigs)
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

      val resolver = TopicResolver()
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
  })
