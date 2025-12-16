package io.github.osoykan.kafkaflow.example.infra

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.osoykan.kafkaflow.example.domain.DomainEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

/**
 * Configured Jackson ObjectMapper for the example application.
 */
val objectMapper: ObjectMapper = jacksonObjectMapper().apply {
  registerModule(JavaTimeModule())
  disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
  disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
}

/**
 * Kafka serializer that uses Jackson to serialize domain events to JSON.
 */
class JacksonSerializer<T> : Serializer<T> {
  override fun serialize(topic: String, data: T): ByteArray = data.let { objectMapper.writeValueAsBytes(it) }
}

/**
 * Kafka deserializer that uses Jackson to deserialize JSON to domain events.
 *
 * Uses @JsonTypeInfo on DomainEvent to handle polymorphic deserialization.
 */
class JacksonDeserializer : Deserializer<DomainEvent> {
  override fun deserialize(
    topic: String,
    data: ByteArray
  ): DomainEvent = data.let { objectMapper.readValue(it, DomainEvent::class.java) }
}
