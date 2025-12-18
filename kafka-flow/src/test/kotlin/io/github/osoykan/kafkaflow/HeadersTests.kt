package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders

class HeadersTests :
  FunSpec({

    test("getHeaderString should return header value as string") {
      val headers = RecordHeaders().apply {
        add(RecordHeader("test-key", "test-value".toByteArray()))
      }
      val record = ConsumerRecord(
        "topic",
        0,
        0L,
        System.currentTimeMillis(),
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        "value",
        headers,
        java.util.Optional.empty()
      )

      record.getHeaderString("test-key") shouldBe "test-value"
      record.getHeaderString("non-existent") shouldBe null
    }

    test("getHeaderInt should parse integer header") {
      val headers = RecordHeaders().apply {
        add(RecordHeader("count", "42".toByteArray()))
        add(RecordHeader("invalid", "not-a-number".toByteArray()))
      }
      val record = ConsumerRecord(
        "topic",
        0,
        0L,
        System.currentTimeMillis(),
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        "value",
        headers,
        java.util.Optional.empty()
      )

      record.getHeaderInt("count") shouldBe 42
      record.getHeaderInt("invalid") shouldBe null
      record.getHeaderInt("missing") shouldBe null
    }

    test("getHeaderLong should parse long header") {
      val headers = RecordHeaders().apply {
        add(RecordHeader("timestamp", "1702900000000".toByteArray()))
      }
      val record = ConsumerRecord(
        "topic",
        0,
        0L,
        System.currentTimeMillis(),
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        "value",
        headers,
        java.util.Optional.empty()
      )

      record.getHeaderLong("timestamp") shouldBe 1702900000000L
      record.getHeaderLong("missing") shouldBe null
    }

    test("addHeader should add string header to ProducerRecord") {
      val record = ProducerRecord<String, String>("topic", "key", "value")
      record.addHeader("custom", "data")

      val headerValue = record
        .headers()
        .lastHeader("custom")
        ?.value()
        ?.let { String(it) }
      headerValue shouldBe "data"
    }

    test("addHeader should add int header to ProducerRecord") {
      val record = ProducerRecord<String, String>("topic", "key", "value")
      record.addHeader("retry-count", 5)

      val headerValue = record
        .headers()
        .lastHeader("retry-count")
        ?.value()
        ?.let { String(it) }
      headerValue shouldBe "5"
    }

    test("addHeader should add long header to ProducerRecord") {
      val record = ProducerRecord<String, String>("topic", "key", "value")
      record.addHeader("timestamp", 1702900000000L)

      val headerValue = record
        .headers()
        .lastHeader("timestamp")
        ?.value()
        ?.let { String(it) }
      headerValue shouldBe "1702900000000"
    }
  })
