package io.github.osoykan.kafkaflow

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.apache.kafka.clients.consumer.ConsumerConfig as KafkaConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig as KafkaProducerConfig

/**
 * Factory utilities for creating Spring Kafka components.
 */
object KafkaFlowFactories {
  /**
   * Creates a consumer factory with the given configuration and deserializers.
   *
   * @param config The Kafka flow configuration
   * @param keyDeserializer Key deserializer instance
   * @param valueDeserializer Value deserializer instance
   * @return Configured consumer factory
   */
  fun <K : Any, V : Any> createConsumerFactory(
    config: KafkaFlowConfig,
    keyDeserializer: Deserializer<K>,
    valueDeserializer: Deserializer<V>
  ): ConsumerFactory<K, V> {
    val props = buildConsumerProperties(config)
    return DefaultKafkaConsumerFactory(
      props,
      keyDeserializer,
      valueDeserializer
    )
  }

  /**
   * Creates a consumer factory with deserializer classes.
   *
   * @param config The Kafka flow configuration
   * @param keyDeserializerClass Key deserializer class
   * @param valueDeserializerClass Value deserializer class
   * @return Configured consumer factory
   */
  fun <K : Any, V : Any> createConsumerFactory(
    config: KafkaFlowConfig,
    keyDeserializerClass: Class<out Deserializer<K>>,
    valueDeserializerClass: Class<out Deserializer<V>>
  ): ConsumerFactory<K, V> {
    val props = buildConsumerProperties(config).apply {
      put(KafkaConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass)
      put(KafkaConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass)
    }
    return DefaultKafkaConsumerFactory(props)
  }

  /**
   * Creates a producer factory with the given configuration and serializers.
   *
   * @param config The Kafka flow configuration
   * @param keySerializer Key serializer instance
   * @param valueSerializer Value serializer instance
   * @return Configured producer factory
   */
  fun <K : Any, V : Any> createProducerFactory(
    config: KafkaFlowConfig,
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
  ): ProducerFactory<K, V> {
    val props = buildProducerProperties(config)
    return DefaultKafkaProducerFactory(
      props,
      keySerializer,
      valueSerializer
    )
  }

  /**
   * Creates a producer factory with serializer classes.
   *
   * @param config The Kafka flow configuration
   * @param keySerializerClass Key serializer class
   * @param valueSerializerClass Value serializer class
   * @return Configured producer factory
   */
  fun <K : Any, V : Any> createProducerFactory(
    config: KafkaFlowConfig,
    keySerializerClass: Class<out Serializer<K>>,
    valueSerializerClass: Class<out Serializer<V>>
  ): ProducerFactory<K, V> {
    val props = buildProducerProperties(config).apply {
      put(KafkaProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass)
      put(KafkaProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass)
    }
    return DefaultKafkaProducerFactory(props)
  }

  /**
   * Creates a KafkaTemplate from a producer factory.
   *
   * @param producerFactory The producer factory to use
   * @return Configured KafkaTemplate
   */
  fun <K : Any, V : Any> createKafkaTemplate(producerFactory: ProducerFactory<K, V>): KafkaTemplate<K, V> =
    KafkaTemplate(producerFactory)

  /**
   * Creates a KafkaTemplate with the given configuration and serializers.
   *
   * @param config The Kafka flow configuration
   * @param keySerializer Key serializer instance
   * @param valueSerializer Value serializer instance
   * @return Configured KafkaTemplate
   */
  fun <K : Any, V : Any> createKafkaTemplate(
    config: KafkaFlowConfig,
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
  ): KafkaTemplate<K, V> {
    val producerFactory = createProducerFactory(config, keySerializer, valueSerializer)
    return createKafkaTemplate(producerFactory)
  }

  private fun buildConsumerProperties(config: KafkaFlowConfig): MutableMap<String, Any> {
    val consumer = config.consumer
    return mutableMapOf(
      KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
      KafkaConsumerConfig.GROUP_ID_CONFIG to consumer.groupId,
      KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG to consumer.autoOffsetReset,
      KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to consumer.enableAutoCommit,
      KafkaConsumerConfig.MAX_POLL_RECORDS_CONFIG to consumer.maxPollRecords,
      KafkaConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to consumer.maxPollInterval.inWholeMilliseconds.toInt(),
      KafkaConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to consumer.sessionTimeout.inWholeMilliseconds.toInt(),
      KafkaConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to consumer.heartbeatInterval.inWholeMilliseconds.toInt(),
      KafkaConsumerConfig.FETCH_MIN_BYTES_CONFIG to consumer.fetchMinBytes,
      KafkaConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to consumer.fetchMaxWait.inWholeMilliseconds.toInt(),
      KafkaConsumerConfig.ISOLATION_LEVEL_CONFIG to consumer.isolationLevel,
      KafkaConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to listOf(consumer.partitionAssignmentStrategy)
    )
  }

  private fun buildProducerProperties(config: KafkaFlowConfig): MutableMap<String, Any> {
    val producer = config.producer
    return mutableMapOf(
      KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
      KafkaProducerConfig.ACKS_CONFIG to producer.acks,
      KafkaProducerConfig.RETRIES_CONFIG to producer.retries,
      KafkaProducerConfig.BATCH_SIZE_CONFIG to producer.batchSize,
      KafkaProducerConfig.LINGER_MS_CONFIG to producer.lingerMs.toInt(),
      KafkaProducerConfig.BUFFER_MEMORY_CONFIG to producer.bufferMemory,
      KafkaProducerConfig.COMPRESSION_TYPE_CONFIG to producer.compressionType,
      KafkaProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to producer.idempotence,
      KafkaProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to producer.maxInFlightRequestsPerConnection,
      KafkaProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to producer.deliveryTimeout.inWholeMilliseconds.toInt(),
      KafkaProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to producer.requestTimeout.inWholeMilliseconds.toInt()
    )
  }
}
