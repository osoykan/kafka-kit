package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RetryPolicyTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("withRetry should succeed on first attempt") {
      val policy = RetryPolicy(maxInMemoryRetries = 3)
      var attempts = 0

      val result = withRetry(policy) {
        attempts++
        "success"
      }

      result.isSuccess shouldBe true
      result.getOrNull() shouldBe "success"
      attempts shouldBe 1
    }

    test("withRetry should retry on failure and eventually succeed") {
      val policy = RetryPolicy(
        maxInMemoryRetries = 3,
        inMemoryBackoff = BackoffStrategy.Fixed(10.milliseconds)
      )
      var attempts = 0

      val result = withRetry(policy) {
        attempts++
        if (attempts < 3) {
          throw RuntimeException("Attempt $attempts failed")
        }
        "success after retries"
      }

      result.isSuccess shouldBe true
      result.getOrNull() shouldBe "success after retries"
      attempts shouldBe 3
    }

    test("withRetry should fail after exhausting retries") {
      val policy = RetryPolicy(
        maxInMemoryRetries = 2,
        inMemoryBackoff = BackoffStrategy.None
      )
      var attempts = 0

      val result = withRetry(policy) {
        attempts++
        throw RuntimeException("Always fails")
      }

      result.isFailure shouldBe true
      attempts shouldBe 3 // initial + 2 retries
    }

    test("exponential backoff should calculate correct delays") {
      val backoff = BackoffStrategy.Exponential(
        initialDelay = 100.milliseconds,
        multiplier = 2.0,
        maxDelay = 10.seconds
      )

      backoff.delayFor(0) shouldBe 100.milliseconds
      backoff.delayFor(1) shouldBe 200.milliseconds
      backoff.delayFor(2) shouldBe 400.milliseconds
      backoff.delayFor(3) shouldBe 800.milliseconds
      backoff.delayFor(10) shouldBe 10.seconds // capped at maxDelay
    }

    test("fixed backoff should return constant delay") {
      val backoff = BackoffStrategy.Fixed(500.milliseconds)

      backoff.delayFor(0) shouldBe 500.milliseconds
      backoff.delayFor(1) shouldBe 500.milliseconds
      backoff.delayFor(5) shouldBe 500.milliseconds
    }

    test("RetryableProcessor should succeed on first attempt") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(kafkaTemplate, RetryPolicy.DEFAULT)

      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) { rec ->
        rec.value().uppercase()
      }

      val success = result.shouldBeInstanceOf<ProcessingResult.Success<String>>()
      success.value shouldBe "VALUE"
    }

    test("RetryableProcessor should retry and succeed after failures") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val policy = RetryPolicy(
        maxInMemoryRetries = 3,
        inMemoryBackoff = BackoffStrategy.Fixed(10.milliseconds)
      )
      val processor = RetryableProcessor(kafkaTemplate, policy)

      var attempts = 0
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        attempts++
        if (attempts < 3) {
          throw RuntimeException("Temporary failure")
        }
        "processed"
      }

      result.shouldBeInstanceOf<ProcessingResult.Success<String>>()
      attempts shouldBe 3
    }

    test("RetryableProcessor should send to retry topic after in-memory retries exhausted") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      kafka.createTopics(topic, retryTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val policy = RetryPolicy(
        maxInMemoryRetries = 2,
        inMemoryBackoff = BackoffStrategy.None,
        retryTopicSuffix = ".retry"
      )
      val processor = RetryableProcessor(kafkaTemplate, policy)

      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        throw RuntimeException("Always fails")
      }

      val sentToRetry = result.shouldBeInstanceOf<ProcessingResult.SentToRetryTopic>()
      sentToRetry.topic shouldBe retryTopic
      sentToRetry.attempt shouldBe 1
    }

    test("RetryableProcessor should send to DLT after retry topic attempts exhausted") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, retryTopic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val policy = RetryPolicy(
        maxInMemoryRetries = 1,
        inMemoryBackoff = BackoffStrategy.None,
        maxRetryTopicAttempts = 2,
        retryTopicSuffix = ".retry",
        dltSuffix = ".dlt"
      )
      val processor = RetryableProcessor(kafkaTemplate, policy)

      // Simulate a record from retry topic that has already been retried twice
      val retryRecord = ConsumerRecord(retryTopic, 0, 0L, "key", "value").apply {
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.RETRY_TOPIC_ATTEMPT,
            "2".toByteArray()
          )
        )
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.ORIGINAL_TOPIC,
            topic.toByteArray()
          )
        )
      }

      val result = processor.process(retryRecord) {
        throw RuntimeException("Still failing")
      }

      val sentToDlt = result.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
      sentToDlt.topic shouldBe dltTopic
    }

    test("full retry flow: message → in-memory retries → retry topic → DLT") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, retryTopic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val policy = RetryPolicy(
        maxInMemoryRetries = 2,
        inMemoryBackoff = BackoffStrategy.Fixed(10.milliseconds),
        maxRetryTopicAttempts = 2,
        retryTopicBackoff = BackoffStrategy.Fixed(10.milliseconds),
        retryTopicSuffix = ".retry",
        dltSuffix = ".dlt"
      )
      val processor = RetryableProcessor(kafkaTemplate, policy)

      // Step 1: Process original message - should go to retry topic
      val originalRecord = ConsumerRecord(topic, 0, 0L, "key", "value")
      val result1 = processor.process(originalRecord) {
        throw RuntimeException("First failure")
      }
      result1.shouldBeInstanceOf<ProcessingResult.SentToRetryTopic>()

      // Step 2: Process from retry topic (attempt 1) - should go to retry topic again
      val retryRecord1 = ConsumerRecord(retryTopic, 0, 0L, "key", "value").apply {
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.RETRY_TOPIC_ATTEMPT,
            "1".toByteArray()
          )
        )
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.ORIGINAL_TOPIC,
            topic.toByteArray()
          )
        )
      }
      val result2 = processor.process(retryRecord1) {
        throw RuntimeException("Second failure")
      }
      val sentToRetry2 = result2.shouldBeInstanceOf<ProcessingResult.SentToRetryTopic>()
      sentToRetry2.attempt shouldBe 2

      // Step 3: Process from retry topic (attempt 2) - should go to DLT
      val retryRecord2 = ConsumerRecord(retryTopic, 0, 0L, "key", "value").apply {
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.RETRY_TOPIC_ATTEMPT,
            "2".toByteArray()
          )
        )
        headers().add(
          org.apache.kafka.common.header.internals.RecordHeader(
            Headers.ORIGINAL_TOPIC,
            topic.toByteArray()
          )
        )
      }
      val result3 = processor.process(retryRecord2) {
        throw RuntimeException("Third failure")
      }
      result3.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
    }

    test("processWithRetry extension should work") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = record.processWithRetry(kafkaTemplate, RetryPolicy.DEFAULT) {
        it.value().length
      }

      val success = result.shouldBeInstanceOf<ProcessingResult.Success<Int>>()
      success.value shouldBe 5
    }
  })
