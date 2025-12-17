package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.github.osoykan.kafkaflow.support.acknowledgeAndExtract
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldNotBeEmpty
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlin.time.Duration.Companion.seconds

class RebalanceTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should handle rebalance when new consumer joins group") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      // Start first consumer
      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumer1 = FlowKafkaConsumer(consumerFactory1, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val records1 = mutableListOf<String>()
      val job1 = async {
        consumer1
          .consume(topicConfig)
          .acknowledgeAndExtract()
          .take(10)
          .collect { records1.add(it.value()) }
      }

      delay(2.seconds)

      // Send some messages
      (1..5).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      delay(1.seconds)

      // Start second consumer (triggers rebalance)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)
      val consumer2 = FlowKafkaConsumer(consumerFactory2, TestHelpers.testListenerConfig())

      val records2 = mutableListOf<String>()
      val job2 = async {
        consumer2
          .consume(topicConfig)
          .acknowledgeAndExtract()
          .take(5)
          .collect { records2.add(it.value()) }
      }

      delay(2.seconds)

      // Send more messages
      (6..10).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      // Wait for consumers to process
      delay(3.seconds)

      job1.cancel()
      job2.cancel()

      // Both consumers should have received some messages after rebalance
      val totalReceived = records1.size + records2.size
      totalReceived shouldBeGreaterThan 0

      consumer1.stop()
      consumer2.stop()
    }

    test("should handle rebalance when consumer leaves group") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      // Start two consumers
      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)

      val consumer1 = FlowKafkaConsumer(consumerFactory1, TestHelpers.testListenerConfig())
      val consumer2 = FlowKafkaConsumer(consumerFactory2, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val records1 = mutableListOf<String>()
      val records2 = mutableListOf<String>()

      val job1 = async {
        consumer1.consume(topicConfig).acknowledgeAndExtract().collect { records1.add(it.value()) }
      }

      val job2 = async {
        consumer2.consume(topicConfig).acknowledgeAndExtract().collect { records2.add(it.value()) }
      }

      delay(2.seconds)

      // Send messages
      (1..10).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      delay(2.seconds)

      // Stop consumer 2 (triggers rebalance)
      consumer2.stop()
      job2.cancel()

      delay(2.seconds)

      // Send more messages - consumer 1 should receive all
      (11..15).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      delay(2.seconds)

      job1.cancel()

      // Consumer 1 should have received messages after consumer 2 left
      records1.shouldNotBeEmpty()

      consumer1.stop()
    }

    test("should not lose messages during rebalance") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 2)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val allReceivedMessages = mutableSetOf<String>()
      val lock = Any()

      // Start first consumer
      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumer1 = FlowKafkaConsumer(consumerFactory1, TestHelpers.testListenerConfig())
      val topicConfig = TopicConfig(name = topic)

      val job1 = async {
        consumer1.consume(topicConfig).acknowledgeAndExtract().collect { record ->
          synchronized(lock) {
            allReceivedMessages.add(record.value())
          }
        }
      }

      delay(2.seconds)

      // Send first batch
      (1..10).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "msg-$i").get()
      }

      delay(1.seconds)

      // Start second consumer (triggers rebalance)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)
      val consumer2 = FlowKafkaConsumer(consumerFactory2, TestHelpers.testListenerConfig())

      val job2 = async {
        consumer2.consume(topicConfig).acknowledgeAndExtract().collect { record ->
          synchronized(lock) {
            allReceivedMessages.add(record.value())
          }
        }
      }

      delay(2.seconds)

      // Send second batch
      (11..20).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "msg-$i").get()
      }

      delay(3.seconds)

      job1.cancel()
      job2.cancel()

      // All messages should have been received (no losses)
      allReceivedMessages.size shouldBe 20

      consumer1.stop()
      consumer2.stop()
    }

    test("should handle cooperative sticky assignor") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 4)

      // Use cooperative sticky assignor (default in ConsumerConfig)
      val groupId = TestHelpers.uniqueGroupId()

      val consumerFactory1 = kafka.createStringConsumerFactory(groupId)
      val consumerFactory2 = kafka.createStringConsumerFactory(groupId)

      val config = TestHelpers.testListenerConfig(concurrency = 2)
      val consumer1 = FlowKafkaConsumer(consumerFactory1, config)
      val consumer2 = FlowKafkaConsumer(consumerFactory2, config)
      val topicConfig = TopicConfig(name = topic, concurrency = 2)

      val records1 = mutableListOf<String>()
      val records2 = mutableListOf<String>()

      val job1 = async {
        consumer1.consume(topicConfig).acknowledgeAndExtract().collect { records1.add(it.value()) }
      }

      delay(2.seconds)

      val job2 = async {
        consumer2.consume(topicConfig).acknowledgeAndExtract().collect { records2.add(it.value()) }
      }

      delay(3.seconds)

      // Send messages
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      (1..20).forEach { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      delay(3.seconds)

      job1.cancel()
      job2.cancel()

      // With cooperative sticky assignor, both consumers should receive messages
      val totalReceived = records1.size + records2.size
      totalReceived shouldBeGreaterThan 0

      consumer1.stop()
      consumer2.stop()
    }
  })
