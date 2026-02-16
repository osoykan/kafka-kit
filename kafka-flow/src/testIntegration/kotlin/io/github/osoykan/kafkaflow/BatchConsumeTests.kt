package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.github.osoykan.kafkaflow.support.acknowledgeAndExtract
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.clients.consumer.ConsumerConfig as KafkaConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig as KafkaProducerConfig

class BatchConsumeTests :
  FunSpec({
    val kafka = SharedKafka.instance

    data class TestTopics(
      val main: String,
      val retry: String,
      val dlt: String
    )

    fun createTopics(prefix: String, partitions: Int = 0): TestTopics {
      val main = TestHelpers.uniqueTopicName(prefix)
      val topics = TestTopics(main, "$main.retry", "$main.dlt")
      if (partitions > 0) {
        kafka.createTopics(topics.main, topics.retry, topics.dlt, partitions = partitions)
      } else {
        kafka.createTopics(topics.main, topics.retry, topics.dlt)
      }
      return topics
    }

    fun topicConfigFor(consumer: Consumer<String, String>, topics: TestTopics) = mapOf(
      consumer.consumerName to TopicConfig(topics = listOf(topics.main), retryTopic = topics.retry, dltTopic = topics.dlt)
    )

    fun createFactory(groupId: String, topicConfigs: Map<String, TopicConfig>): KafkaFlowFactory<String, String> =
      KafkaFlowFactory.create(
        KafkaFlowFactoryConfig(
          consumerProperties = mapOf(
            KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            KafkaConsumerConfig.GROUP_ID_CONFIG to groupId,
            KafkaConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            KafkaConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            KafkaConsumerConfig.MAX_POLL_RECORDS_CONFIG to 500
          ),
          producerProperties = mapOf(
            KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            KafkaProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            KafkaProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
          ),
          listenerConfig = TestHelpers.testListenerConfig(),
          topicResolver = DefaultTopicResolver(topicConfigs = topicConfigs)
        )
      )

    suspend fun <T : Consumer<String, String>> withBatchEngine(
      consumer: T,
      topics: TestTopics,
      block: suspend (ConsumerEngine<String, String>) -> Unit
    ) {
      val groupId = TestHelpers.uniqueGroupId()
      val factory = createFactory(groupId, topicConfigFor(consumer, topics))
      val engine = factory.createConsumerEngine(listOf(consumer))
      engine.start()
      delay(2.seconds)
      try {
        block(engine)
      } finally {
        engine.stop()
      }
    }

    test("should consume messages as a batch with auto-ack") {
      val topics = createTopics("batch-auto")
      val receivedRecords = CopyOnWriteArrayList<ConsumerRecord<String, String>>()
      val allReceived = CompletableDeferred<Unit>()

      class TestBatchConsumer : BatchConsumerAutoAck<String, String> {
        override suspend fun consume(
          records: List<ConsumerRecord<String, String>>,
          failureHandler: FailureHandler<String, String>
        ) {
          receivedRecords.addAll(records)
          if (receivedRecords.size >= 5) allReceived.complete(Unit)
        }
      }

      withBatchEngine(TestBatchConsumer(), topics) {
        val kafkaTemplate = kafka.createStringKafkaTemplate()
        (1..5).forEach { i -> kafkaTemplate.send(topics.main, "key-$i", "value-$i").get() }

        withTimeout(30.seconds) { allReceived.await() }
        receivedRecords.map { it.value() } shouldContainAll (1..5).map { "value-$it" }
      }
    }

    test("should route failed records via batch failure handler to DLT") {
      val topics = createTopics("batch-dlt")
      val processedRecords = CopyOnWriteArrayList<String>()
      val dltSent = CompletableDeferred<Unit>()

      class FailingBatchConsumer : BatchConsumerAutoAck<String, String> {
        override suspend fun consume(
          records: List<ConsumerRecord<String, String>>,
          failureHandler: FailureHandler<String, String>
        ) {
          for (record in records) {
            if (record.value().contains("fail")) {
              failureHandler.sendToDlt(record, RuntimeException("Intentional failure"))
              dltSent.complete(Unit)
            } else {
              processedRecords.add(record.value())
            }
          }
        }
      }

      withBatchEngine(FailingBatchConsumer(), topics) {
        val kafkaTemplate = kafka.createStringKafkaTemplate()
        kafkaTemplate.send(topics.main, "key-1", "value-ok").get()
        kafkaTemplate.send(topics.main, "key-2", "value-fail").get()
        kafkaTemplate.send(topics.main, "key-3", "value-ok-2").get()

        withTimeout(30.seconds) { dltSent.await() }
        delay(1.seconds)

        processedRecords shouldContainAll listOf("value-ok", "value-ok-2")

        // Verify DLT record
        val dltConsumerFactory = kafka.createStringConsumerFactory(TestHelpers.uniqueGroupId("dlt-verify"))
        val dltConsumer = FlowKafkaConsumer(dltConsumerFactory, TestHelpers.testListenerConfig())
        val dltJob = async {
          dltConsumer
            .consume(TopicConfig(name = topics.dlt))
            .acknowledgeAndExtract()
            .take(1)
            .toList()
        }
        val dltRecords = withTimeout(30.seconds) { dltJob.await() }
        dltRecords.size shouldBe 1
        dltRecords.first().value() shouldBe "value-fail"
        dltConsumer.stop()
      }
    }

    test("should consume messages with manual-ack batch consumer") {
      val topics = createTopics("batch-manual")
      val receivedRecords = CopyOnWriteArrayList<ConsumerRecord<String, String>>()
      val allReceived = CompletableDeferred<Unit>()

      class ManualAckBatchConsumer : BatchConsumerManualAck<String, String> {
        override suspend fun consume(
          records: List<ConsumerRecord<String, String>>,
          failureHandler: FailureHandler<String, String>,
          ack: Acknowledgment
        ) {
          receivedRecords.addAll(records)
          ack.acknowledge()
          if (receivedRecords.size >= 3) allReceived.complete(Unit)
        }
      }

      withBatchEngine(ManualAckBatchConsumer(), topics) {
        val kafkaTemplate = kafka.createStringKafkaTemplate()
        (1..3).forEach { i -> kafkaTemplate.send(topics.main, "key-$i", "value-$i").get() }

        withTimeout(30.seconds) { allReceived.await() }
        receivedRecords.map { it.value() } shouldContainAll (1..3).map { "value-$it" }
      }
    }

    test("should maintain record ordering within a batch") {
      val topics = createTopics("batch-order", partitions = 1)
      val receivedValues = CopyOnWriteArrayList<String>()
      val allReceived = CompletableDeferred<Unit>()

      class OrderedBatchConsumer : BatchConsumerAutoAck<String, String> {
        override suspend fun consume(
          records: List<ConsumerRecord<String, String>>,
          failureHandler: FailureHandler<String, String>
        ) {
          records.forEach { receivedValues.add(it.value()) }
          if (receivedValues.size >= 10) allReceived.complete(Unit)
        }
      }

      withBatchEngine(OrderedBatchConsumer(), topics) {
        val kafkaTemplate = kafka.createStringKafkaTemplate()
        (1..10).forEach { i -> kafkaTemplate.send(topics.main, "same-key", "value-$i").get() }

        withTimeout(30.seconds) { allReceived.await() }
        receivedValues shouldBe (1..10).map { "value-$it" }
      }
    }

    test("should route failed records to retry topic") {
      val topics = createTopics("batch-retry")
      val retrySent = CompletableDeferred<Unit>()

      class RetryBatchConsumer : BatchConsumerAutoAck<String, String> {
        override suspend fun consume(
          records: List<ConsumerRecord<String, String>>,
          failureHandler: FailureHandler<String, String>
        ) {
          for (record in records) {
            if (record.value().contains("retry-me") && record.topic() == topics.main) {
              failureHandler.sendToRetry(record, RuntimeException("Retry this"))
              retrySent.complete(Unit)
            }
          }
        }
      }

      withBatchEngine(RetryBatchConsumer(), topics) {
        val kafkaTemplate = kafka.createStringKafkaTemplate()
        kafkaTemplate.send(topics.main, "key-1", "retry-me").get()

        withTimeout(30.seconds) { retrySent.await() }

        // Verify retry record
        val retryConsumerFactory = kafka.createStringConsumerFactory(TestHelpers.uniqueGroupId("retry-verify"))
        val retryConsumer = FlowKafkaConsumer(retryConsumerFactory, TestHelpers.testListenerConfig())
        val retryJob = async {
          retryConsumer
            .consume(TopicConfig(name = topics.retry))
            .acknowledgeAndExtract()
            .take(1)
            .toList()
        }
        val retryRecords = withTimeout(30.seconds) { retryJob.await() }
        retryRecords.size shouldBe 1
        retryRecords.first().value() shouldBe "retry-me"
        retryRecords.first().headerAsString(Headers.ORIGINAL_TOPIC) shouldBe topics.main
        retryConsumer.stop()
      }
    }
  })
