package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.seconds

class ProduceConsumeIntegrationTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should produce and consume messages end-to-end") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val consumer = FlowKafkaConsumer(consumerFactory, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val consumeJob = async {
        consumer.consume(topicConfig).take(5).toList()
      }

      delay(1.seconds)

      // Send messages
      (1..5).forEach { i ->
        producer.send(topic, "key-$i", "value-$i")
      }

      val records = consumeJob.await()

      records shouldHaveSize 5
      records.map { it.value() } shouldContainAll (1..5).map { "value-$it" }

      consumer.stop()
    }

    test("should handle high throughput - 1k messages") {
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
      val receivedMessages = mutableSetOf<String>()

      val consumeJob = async {
        consumer.consume(topicConfig).take(messageCount).collect { record ->
          receivedMessages.add(record.value())
        }
      }

      delay(2.seconds)

      // Send messages in batches
      (1..messageCount).chunked(100).forEach { batch ->
        batch.forEach { i ->
          producer.send(topic, "key-$i", "value-$i")
        }
      }

      consumeJob.await()

      receivedMessages shouldHaveSize messageCount

      consumer.stop()
    }

    test("should maintain message ordering within partition") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val consumer = FlowKafkaConsumer(consumerFactory, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val consumeJob = async {
        consumer.consume(topicConfig).take(10).toList()
      }

      delay(1.seconds)

      // Send messages with same key to ensure same partition
      (1..10).forEach { i ->
        producer.send(topic, partition = 0, key = "same-key", value = "value-$i")
      }

      val records = consumeJob.await()

      // Verify ordering
      records.map { it.value() } shouldBe (1..10).map { "value-$it" }

      consumer.stop()
    }

    test("should handle multiple topics simultaneously") {
      val topic1 = TestHelpers.uniqueTopicName("topic-1")
      val topic2 = TestHelpers.uniqueTopicName("topic-2")
      kafka.createTopics(topic1, topic2)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val config = TestHelpers.testListenerConfig()
      val consumer1 = FlowKafkaConsumer(consumerFactory1, config)
      val consumer2 = FlowKafkaConsumer(consumerFactory2, config)

      val records1 = mutableListOf<String>()
      val records2 = mutableListOf<String>()

      val job1 = async {
        consumer1.consume(TopicConfig(topic1)).take(5).collect { records1.add(it.value()) }
      }

      val job2 = async {
        consumer2.consume(TopicConfig(topic2)).take(5).collect { records2.add(it.value()) }
      }

      delay(1.seconds)

      // Send to both topics
      (1..5).forEach { i ->
        producer.send(topic1, "key-$i", "topic1-value-$i")
        producer.send(topic2, "key-$i", "topic2-value-$i")
      }

      job1.await()
      job2.await()

      records1 shouldHaveSize 5
      records2 shouldHaveSize 5
      records1.all { it.startsWith("topic1-") } shouldBe true
      records2.all { it.startsWith("topic2-") } shouldBe true

      consumer1.stop()
      consumer2.stop()
    }

    test("should work with byte array serialization") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createByteArrayConsumerFactory(groupId)
      val kafkaTemplate = kafka.createByteArrayKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val consumer = FlowKafkaConsumer(consumerFactory, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val consumeJob = async {
        consumer.consume(topicConfig).take(3).toList()
      }

      delay(1.seconds)

      // Send byte array messages
      (1..3).forEach { i ->
        producer.send(topic, "key-$i", "value-$i".toByteArray())
      }

      val records = consumeJob.await()

      records shouldHaveSize 3
      records.map { String(it.value()) } shouldContainAll (1..3).map { "value-$it" }

      consumer.stop()
    }

    test("should handle message headers in produce-consume flow") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val producer = FlowKafkaProducer(kafkaTemplate)
      val consumer = FlowKafkaConsumer(consumerFactory, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val consumeJob = async {
        consumer.consume(topicConfig).take(1).toList()
      }

      delay(1.seconds)

      val headers = mapOf(
        "correlation-id" to "test-123".toByteArray(),
        "source" to "integration-test".toByteArray()
      )
      producer.send(topic, "key-1", "value-1", headers)

      val records = consumeJob.await()

      records shouldHaveSize 1
      val record = records.first()
      record.headerAsString("correlation-id") shouldBe "test-123"
      record.headerAsString("source") shouldBe "integration-test"

      consumer.stop()
    }
  })
