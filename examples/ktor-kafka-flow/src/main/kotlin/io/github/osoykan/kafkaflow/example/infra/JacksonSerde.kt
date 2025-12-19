package io.github.osoykan.kafkaflow.example.infra

import io.github.osoykan.kafkaflow.example.domain.DomainEvent
import org.apache.kafka.common.serialization.*
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.cfg.DateTimeFeature
import tools.jackson.module.kotlin.*

/**
 * Configured Jackson ObjectMapper for the example application.
 */
val objectMapper = jsonMapper {
  addModule(kotlinModule())
  findAndAddModules()
  disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  disable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)
  disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
  disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  enable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
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
