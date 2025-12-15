package io.github.osoykan.kafkaflow.support

import io.github.osoykan.kafkaflow.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.*
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * Abstract interface for a Kafka broker used in testing.
 * Implementations can be EmbeddedKafka (fast, local TDD) or Testcontainers (CI).
 */
interface KafkaBroker {
  val bootstrapServers: String
  val isRunning: Boolean

  fun start()

  fun stop()

  fun createTopic(name: String, partitions: Int = 3, replicationFactor: Short = 1)

  fun createTopics(vararg names: String, partitions: Int = 3, replicationFactor: Short = 1)

  fun deleteTopic(name: String)

  fun listTopics(): Set<String>
}

/**
 * Abstract base class with common factory creation methods.
 */
abstract class AbstractKafkaBroker : KafkaBroker {
  protected var adminClient: AdminClient? = null

  protected fun initAdminClient() {
    adminClient = AdminClient.create(
      mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
    )
  }

  protected fun closeAdminClient() {
    adminClient?.close()
    adminClient = null
  }

  override fun createTopic(name: String, partitions: Int, replicationFactor: Short) {
    val topic = NewTopic(name, partitions, replicationFactor)
    adminClient?.createTopics(listOf(topic))?.all()?.get(30, TimeUnit.SECONDS)
  }

  override fun createTopics(vararg names: String, partitions: Int, replicationFactor: Short) {
    val topics = names.map { NewTopic(it, partitions, replicationFactor) }
    adminClient?.createTopics(topics)?.all()?.get(30, TimeUnit.SECONDS)
  }

  override fun deleteTopic(name: String) {
    adminClient?.deleteTopics(listOf(name))?.all()?.get(30, TimeUnit.SECONDS)
  }

  override fun listTopics(): Set<String> =
    adminClient?.listTopics()?.names()?.get(30, TimeUnit.SECONDS) ?: emptySet()

  /**
   * Creates a default KafkaFlowConfig for testing.
   */
  fun createConfig(
    groupId: String = "test-group-${UUID.randomUUID()}",
    listenerConfig: ListenerConfig = ListenerConfig(concurrency = 1, pollTimeout = 1.seconds)
  ): KafkaFlowConfig = KafkaFlowConfig(
    bootstrapServers = bootstrapServers,
    producer = ProducerConfig(),
    consumer = ConsumerConfig(groupId = groupId),
    listener = listenerConfig
  )

  /**
   * Creates a consumer factory for String keys and values.
   */
  fun createStringConsumerFactory(
    groupId: String = "test-group-${UUID.randomUUID()}"
  ): ConsumerFactory<String, String> {
    val config = createConfig(groupId)
    return KafkaFlowFactories.createConsumerFactory(
      config,
      StringDeserializer(),
      StringDeserializer()
    )
  }

  /**
   * Creates a consumer factory for String keys and ByteArray values.
   */
  fun createByteArrayConsumerFactory(
    groupId: String = "test-group-${UUID.randomUUID()}"
  ): ConsumerFactory<String, ByteArray> {
    val config = createConfig(groupId)
    return KafkaFlowFactories.createConsumerFactory(
      config,
      StringDeserializer(),
      ByteArrayDeserializer()
    )
  }

  /**
   * Creates a consumer factory with custom deserializers.
   */
  fun <K : Any, V : Any> createConsumerFactory(
    keyDeserializer: Deserializer<K>,
    valueDeserializer: Deserializer<V>,
    groupId: String = "test-group-${UUID.randomUUID()}"
  ): ConsumerFactory<K, V> {
    val config = createConfig(groupId)
    return KafkaFlowFactories.createConsumerFactory(config, keyDeserializer, valueDeserializer)
  }

  /**
   * Creates a producer factory for String keys and values.
   */
  fun createStringProducerFactory(): ProducerFactory<String, String> {
    val config = createConfig()
    return KafkaFlowFactories.createProducerFactory(
      config,
      StringSerializer(),
      StringSerializer()
    )
  }

  /**
   * Creates a producer factory for String keys and ByteArray values.
   */
  fun createByteArrayProducerFactory(): ProducerFactory<String, ByteArray> {
    val config = createConfig()
    return KafkaFlowFactories.createProducerFactory(
      config,
      StringSerializer(),
      ByteArraySerializer()
    )
  }

  /**
   * Creates a producer factory with custom serializers.
   */
  fun <K : Any, V : Any> createProducerFactory(
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
  ): ProducerFactory<K, V> {
    val config = createConfig()
    return KafkaFlowFactories.createProducerFactory(config, keySerializer, valueSerializer)
  }

  /**
   * Creates a KafkaTemplate for String keys and values.
   */
  fun createStringKafkaTemplate(): KafkaTemplate<String, String> =
    KafkaFlowFactories.createKafkaTemplate(createStringProducerFactory())

  /**
   * Creates a KafkaTemplate for String keys and ByteArray values.
   */
  fun createByteArrayKafkaTemplate(): KafkaTemplate<String, ByteArray> =
    KafkaFlowFactories.createKafkaTemplate(createByteArrayProducerFactory())

  /**
   * Creates a KafkaTemplate with custom serializers.
   */
  fun <K : Any, V : Any> createKafkaTemplate(
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
  ): KafkaTemplate<K, V> =
    KafkaFlowFactories.createKafkaTemplate(createProducerFactory(keySerializer, valueSerializer))
}
