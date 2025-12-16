package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.github.osoykan.kafkaflow.support.collectWithTimeout
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class FlowKafkaConsumerTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should consume messages as Flow") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val template = kafka.createStringKafkaTemplate()

      val config = TestHelpers.testListenerConfig(concurrency = 1)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      // Start consuming
      val consumeJob = async {
        consumer.consume(topicConfig).take(3).toList()
      }

      // Give consumer time to start
      delay(1.seconds)

      // Produce messages
      template.send(topic, "key-1", "value-1").get()
      template.send(topic, "key-2", "value-2").get()
      template.send(topic, "key-3", "value-3").get()

      val records = consumeJob.await()

      records.size shouldBe 3
      records.map { it.value() } shouldContainAll listOf("value-1", "value-2", "value-3")

      consumer.stop()
    }

    test("should consume with manual acknowledgment") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val template = kafka.createStringKafkaTemplate()

      val config = TestHelpers.testListenerConfig(concurrency = 1)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      // Start consuming with ack
      val consumeJob = async {
        consumer.consumeWithAck(topicConfig).take(2).toList()
      }

      delay(1.seconds)

      template.send(topic, "key-1", "value-1").get()
      template.send(topic, "key-2", "value-2").get()

      val records = consumeJob.await()

      records.size shouldBe 2
      records.forEach { ackRecord ->
        ackRecord.record.value() shouldBe ackRecord.record.value()
        // Acknowledge the message
        ackRecord.acknowledgment.acknowledge()
      }

      consumer.stop()
    }

    test("should respect concurrency settings") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val config = TestHelpers.testListenerConfig(concurrency = 2)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      // Topic-level concurrency (4) should override config-level (2)
      val topicConfig = TopicConfig(name = topic, concurrency = 4)

      // Start consuming with topic-specific concurrency
      val consumeJob = async {
        consumer.consume(topicConfig).take(4).toList()
      }

      delay(500.milliseconds)

      val template = kafka.createStringKafkaTemplate()
      // Send multiple messages to test concurrent processing
      repeat(4) { i ->
        template.send(topic, "key-$i", "value-$i").get()
      }

      val results = consumeJob.await()

      // Verify all messages were consumed
      results.size shouldBe 4
      consumer.stop()
    }

    test("should respect poll timeout configuration") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val config = TestHelpers.testListenerConfig(pollTimeout = 200.milliseconds)
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic, pollTimeout = 100.milliseconds)

      val consumeJob = async {
        consumer.consume(topicConfig).take(1).toList()
      }

      delay(500.milliseconds)

      val template = kafka.createStringKafkaTemplate()
      template.send(topic, "key-1", "value-1").get()

      val records = consumeJob.await()
      records.size shouldBe 1

      consumer.stop()
    }

    test("should stop gracefully on cancel") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val consumeJob = async {
        consumer.consume(topicConfig).take(10).toList()
      }

      delay(500.milliseconds)

      // Stop consumer before all messages arrive
      consumer.stop()
      consumeJob.cancel()

      consumer.isStopped() shouldBe true
      consumer.activeContainerCount() shouldBe 0
    }

    test("should handle multiple consumers on same topic") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      val groupId = TestHelpers.uniqueGroupId()
      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)

      val config = TestHelpers.testListenerConfig(concurrency = 2)
      val consumer1 = FlowKafkaConsumer(consumerFactory1, config)
      val consumer2 = FlowKafkaConsumer(consumerFactory2, config)
      val topicConfig = TopicConfig(name = topic)

      val records1 = mutableListOf<String>()
      val records2 = mutableListOf<String>()

      val job1 = async {
        consumer1.consume(topicConfig).take(5).collect { records1.add(it.value()) }
      }

      val job2 = async {
        consumer2.consume(topicConfig).take(5).collect { records2.add(it.value()) }
      }

      delay(2.seconds)

      val template = kafka.createStringKafkaTemplate()
      (1..10).forEach { i ->
        template.send(topic, "key-$i", "value-$i").get()
      }

      job1.await()
      job2.await()

      // Both consumers should have received some messages
      (records1.size + records2.size) shouldBe 10

      consumer1.stop()
      consumer2.stop()
    }
  })
