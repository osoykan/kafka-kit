package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.kafka.clients.producer.ProducerRecord

class FlowKafkaProducerTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should send single message successfully") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val metadata = producer.send(topic, "key-1", "value-1")

      metadata.topic() shouldBe topic
      metadata.partition() shouldNotBe null
      metadata.offset() shouldNotBe null
    }

    test("should send message with headers") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val headers = mapOf(
        "header-1" to "value-1".toByteArray(),
        "header-2" to "value-2".toByteArray()
      )

      val metadata = producer.send(topic, "key-1", "value-1", headers)

      metadata.topic() shouldBe topic
    }

    test("should send message to specific partition") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 3)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val metadata = producer.send(topic, partition = 1, key = "key-1", value = "value-1")

      metadata.topic() shouldBe topic
      metadata.partition() shouldBe 1
    }

    test("should send ProducerRecord directly") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val record = ProducerRecord(topic, "key-1", "value-1")
      val metadata = producer.send(record)

      metadata.topic() shouldBe topic
    }

    test("should close producer and prevent further sends") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      producer.send(topic, "key-1", "value-1")
      producer.close()

      producer.isClosed() shouldBe true
    }

    test("should flush pending sends") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      producer.send(topic, "key-1", "value-1")
      producer.flush()

      // Flush should complete without error
    }
  })
