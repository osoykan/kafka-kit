package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class TtlAndClassifierRetryTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("RetryableProcessor should skip to DLT for non-retryable exceptions") {
      val topic = TestHelpers.uniqueTopicName()
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(maxInMemoryRetries = 5),
        classifier = DefaultExceptionClassifier() // IllegalArgumentException is non-retryable
      )

      var attempts = 0
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        attempts++
        throw IllegalArgumentException("Validation error - not retryable")
      }

      result.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
      attempts shouldBe 1 // Only one attempt because exception is non-retryable
    }

    test("RetryableProcessor should retry for retryable exceptions") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      kafka.createTopics(topic, retryTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(
          maxInMemoryRetries = 3,
          inMemoryBackoff = BackoffStrategy.None
        ),
        classifier = DefaultExceptionClassifier() // RuntimeException is retryable
      )

      var attempts = 0
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        attempts++
        throw RuntimeException("Temporary error - retryable")
      }

      result.shouldBeInstanceOf<ProcessingResult.SentToRetryTopic>()
      attempts shouldBe 4 // 1 initial + 3 retries
    }

    test("RetryableProcessor should expire message when max retry duration exceeded") {
      val topic = TestHelpers.uniqueTopicName()
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(
          maxInMemoryRetries = 3,
          maxRetryDuration = 1.milliseconds // Very short duration
        )
      )

      // Create a record that appears to have been in retry for too long
      val record = ConsumerRecord(topic, 0, 0L, "key", "value").apply {
        headers().add(
          RecordHeader(
            Headers.FIRST_FAILURE_TIME,
            (System.currentTimeMillis() - 10_000).toString().toByteArray() // 10 seconds ago
          )
        )
      }

      val result = processor.process(record) { "should not be called" }

      val expired = result.shouldBeInstanceOf<ProcessingResult.Expired>()
      expired.reason.contains("Max retry duration exceeded") shouldBe true
    }

    test("RetryableProcessor should expire message when max message age exceeded") {
      val topic = TestHelpers.uniqueTopicName()
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(
          maxInMemoryRetries = 3,
          maxMessageAge = 1.milliseconds // Very short age
        )
      )

      // Create a record with a very old timestamp
      val oldTimestamp = System.currentTimeMillis() - 10_000 // 10 seconds ago
      val record = ConsumerRecord(
        topic,
        0,
        0L,
        oldTimestamp,
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        "value",
        org.apache.kafka.common.header.internals
          .RecordHeaders(),
        java.util.Optional.empty()
      )

      val result = processor.process(record) { "should not be called" }

      val expired = result.shouldBeInstanceOf<ProcessingResult.Expired>()
      expired.reason.contains("Max message age exceeded") shouldBe true
    }

    test("RetryableProcessor should process normally when TTL not exceeded") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(
          maxInMemoryRetries = 3,
          maxRetryDuration = 10.seconds, // Reasonable duration
          maxMessageAge = 10.seconds // Reasonable age
        )
      )

      // Create record with current timestamp
      val currentTimestamp = System.currentTimeMillis()
      val record = ConsumerRecord(
        topic,
        0,
        0L,
        currentTimestamp,
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        "value",
        org.apache.kafka.common.header.internals
          .RecordHeaders(),
        java.util.Optional.empty()
      )

      val result = processor.process(record) { rec -> rec.value().uppercase() }

      val success = result.shouldBeInstanceOf<ProcessingResult.Success<String>>()
      success.value shouldBe "VALUE"
    }

    test("AlwaysRetryClassifier should retry all exceptions including validation errors") {
      val topic = TestHelpers.uniqueTopicName()
      val retryTopic = "$topic.retry"
      kafka.createTopics(topic, retryTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(
          maxInMemoryRetries = 2,
          inMemoryBackoff = BackoffStrategy.None
        ),
        classifier = AlwaysRetryClassifier
      )

      var attempts = 0
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        attempts++
        throw IllegalArgumentException("This would normally be non-retryable")
      }

      result.shouldBeInstanceOf<ProcessingResult.SentToRetryTopic>()
      attempts shouldBe 3 // 1 initial + 2 retries (because AlwaysRetryClassifier)
    }

    test("NeverRetryClassifier should send all exceptions to DLT immediately") {
      val topic = TestHelpers.uniqueTopicName()
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val processor = RetryableProcessor(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(maxInMemoryRetries = 5),
        classifier = NeverRetryClassifier
      )

      var attempts = 0
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      val result = processor.process(record) {
        attempts++
        throw RuntimeException("This would normally be retryable")
      }

      result.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
      attempts shouldBe 1 // Only one attempt
    }

    test("processWithRetry extension should accept classifier parameter") {
      val topic = TestHelpers.uniqueTopicName()
      val dltTopic = "$topic.dlt"
      kafka.createTopics(topic, dltTopic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val record = ConsumerRecord(topic, 0, 0L, "key", "value")

      var attempts = 0
      val result = record.processWithRetry(
        kafkaTemplate = kafkaTemplate,
        policy = RetryPolicy(maxInMemoryRetries = 3),
        classifier = NeverRetryClassifier
      ) {
        attempts++
        throw RuntimeException("error")
      }

      result.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
      attempts shouldBe 1
    }
  })
