package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.AckableRecord
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.concurrent.CompletableFuture

class RetryableProcessorTests :
  FunSpec({

    test("RetryableProcessor should publish to the configured retry topic") {
      val kafkaTemplate = RecordingKafkaTemplate()
      val supervisor = testSupervisor(
        kafkaTemplate = kafkaTemplate,
        config = ResolvedConsumerConfig(
          topic = TopicConfig(
            name = "orders",
            retryTopic = "orders-custom.retry",
            dltTopic = "orders-custom.dlt"
          ),
          retry = RetryPolicy(maxInMemoryRetries = 0),
          classifier = AlwaysRetryClassifier,
          consumerName = "RetryTopicConsumer"
        ),
        failure = RuntimeException("boom")
      )

      val result = supervisor.process(ConsumerRecord("orders", 0, 0L, "key", "value"))

      result shouldBe ProcessingResult.SentToRetryTopic("orders-custom.retry", 1)
      kafkaTemplate.records.single().topic() shouldBe "orders-custom.retry"
    }

    test("RetryableProcessor should publish to the configured DLT topic") {
      val kafkaTemplate = RecordingKafkaTemplate()
      val supervisor = testSupervisor(
        kafkaTemplate = kafkaTemplate,
        config = ResolvedConsumerConfig(
          topic = TopicConfig(
            name = "payments",
            retryTopic = "payments-custom.retry",
            dltTopic = "payments-custom.dlt"
          ),
          retry = RetryPolicy(maxInMemoryRetries = 0),
          classifier = NeverRetryClassifier,
          consumerName = "DltConsumer"
        ),
        failure = IllegalStateException("boom")
      )

      val result = supervisor.process(ConsumerRecord("payments", 0, 0L, "key", "value"))

      val sentToDlt = result.shouldBeInstanceOf<ProcessingResult.SentToDlt>()
      sentToDlt.topic shouldBe "payments-custom.dlt"
      sentToDlt.reason shouldBe "Non-retryable exception"
      kafkaTemplate.records.single().topic() shouldBe "payments-custom.dlt"
    }
  })

private class RecordingKafkaTemplate : KafkaTemplate<String, String>(DefaultKafkaProducerFactory(emptyMap())) {
  val records = mutableListOf<ProducerRecord<String, String>>()

  override fun send(record: ProducerRecord<String, String>): CompletableFuture<SendResult<String, String>> {
    records += record
    val metadata = RecordMetadata(
      TopicPartition(record.topic(), record.partition() ?: 0),
      0L,
      0,
      System.currentTimeMillis(),
      0,
      0
    )
    return CompletableFuture.completedFuture(SendResult(record, metadata))
  }
}

private class TestConsumerSupervisor(
  config: ResolvedConsumerConfig,
  kafkaTemplate: KafkaTemplate<String, String>,
  private val failure: Throwable
) : AbstractConsumerSupervisor<String, String>(
    config = config,
    flowConsumer = FlowKafkaConsumer(
      consumerFactory = DefaultKafkaConsumerFactory(emptyMap()),
      listenerConfig = ListenerConfig()
    ),
    kafkaTemplate = kafkaTemplate,
    listenerConfig = ListenerConfig(),
    consumerName = config.consumerName
  ) {
  suspend fun process(record: ConsumerRecord<String, String>): ProcessingResult<*> =
    handleRecord(AckableRecord(record) {})

  override suspend fun handleRecord(ackRecord: AckableRecord<String, String>): ProcessingResult<*> =
    retryProcessor.process(ackRecord.record) { throw failure }
}

private fun testSupervisor(
  kafkaTemplate: KafkaTemplate<String, String>,
  config: ResolvedConsumerConfig,
  failure: Throwable
): TestConsumerSupervisor = TestConsumerSupervisor(config, kafkaTemplate, failure)
