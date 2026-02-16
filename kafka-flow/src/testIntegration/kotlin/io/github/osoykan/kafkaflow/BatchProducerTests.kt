package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.ProducerRecord

class BatchProducerTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should send all records in parallel") {
      val topic = TestHelpers.uniqueTopicName("parallel-send")
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val records = (1..20).map { i ->
        ProducerRecord(topic, "key-$i", "value-$i")
      }

      val results = producer.sendAllParallel(records)

      results.size shouldBe 20
      results.all { it.topic() == topic } shouldBe true
    }

    test("sendAllParallel should be faster than sequential sendAll for many records") {
      val topic = TestHelpers.uniqueTopicName("parallel-perf")
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val records = (1..50).map { i ->
        ProducerRecord(topic, "key-$i", "value-$i")
      }

      val parallelResults = producer.sendAllParallel(records)

      parallelResults.size shouldBe 50
    }
  })
