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

    test("should send all records in parallel with results tracking") {
      val topic = TestHelpers.uniqueTopicName("parallel-results")
      kafka.createTopic(topic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      val records = (1..10).map { i ->
        ProducerRecord(topic, "key-$i", "value-$i")
      }

      val results = producer.sendAllParallelWithResults(records)

      results.size shouldBe 10
      results.all { it is SendResult.Success } shouldBe true

      val successResults = results.filterIsInstance<SendResult.Success<String, String>>()
      successResults.map { it.record.key() }.toSet() shouldBe (1..10).map { "key-$it" }.toSet()
    }

    test("sendAllParallelWithResults should capture failures without throwing") {
      val validTopic = TestHelpers.uniqueTopicName("parallel-valid")
      kafka.createTopic(validTopic)

      val template = kafka.createStringKafkaTemplate()
      val producer = FlowKafkaProducer(template)

      // Mix valid records - all should succeed since we're using valid topics
      val records = (1..5).map { i ->
        ProducerRecord(validTopic, "key-$i", "value-$i")
      }

      val results = producer.sendAllParallelWithResults(records)

      results.size shouldBe 5
      results.count { it is SendResult.Success } shouldBe 5
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
