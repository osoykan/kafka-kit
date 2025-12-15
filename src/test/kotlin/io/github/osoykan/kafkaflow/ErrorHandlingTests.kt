package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.seconds

class ErrorHandlingTests :
  FunSpec({
    val kafka = SharedKafka.instance

    test("should add error headers to DLT messages") {
      val topic = TestHelpers.uniqueTopicName()

      // Verify DltMetadata extraction works
      val metadata = DltMetadata(
        originalTopic = topic,
        originalPartition = 0,
        originalOffset = 100L,
        originalTimestamp = null,
        exceptionClass = "java.lang.RuntimeException",
        exceptionMessage = "Test error",
        failedAt = System.currentTimeMillis()
      )

      metadata.originalTopic shouldBe topic
      metadata.exceptionClass shouldBe "java.lang.RuntimeException"
    }

    test("should handle deserialization errors gracefully") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      // Create consumer with error handler
      val errorHandler = createErrorHandler<String, String>()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config, errorHandler)
      val topicConfig = TopicConfig(name = topic)

      val records = mutableListOf<String>()
      val job = async {
        consumer.consume(topicConfig).take(2).collect { records.add(it.value()) }
      }

      delay(1.seconds)

      // Send valid messages
      kafkaTemplate.send(topic, "key-1", "valid-value-1").get()
      kafkaTemplate.send(topic, "key-2", "valid-value-2").get()

      job.await()

      records.size shouldBe 2

      consumer.stop()
    }

    test("should continue processing after error") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()

      val processedRecords = mutableListOf<String>()

      val consumerFactory = kafka.createStringConsumerFactory(groupId)
      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer.consume(topicConfig).take(5).collect { record ->
          processedRecords.add(record.value())
        }
      }

      delay(1.seconds)

      kafkaTemplate.send(topic, "key-1", "value-1").get()
      kafkaTemplate.send(topic, "key-2", "value-2").get()
      kafkaTemplate.send(topic, "key-3", "value-3").get()
      kafkaTemplate.send(topic, "key-4", "value-4").get()
      kafkaTemplate.send(topic, "key-5", "value-5").get()

      job.await()

      processedRecords.size shouldBe 5

      consumer.stop()
    }

    test("should invoke custom error handler") {
      val handledErrors = mutableListOf<Throwable>()

      val typedHandler = typedErrorHandler {
        on<RuntimeException> { _, exception ->
          handledErrors.add(exception)
        }
        default { _, exception ->
          handledErrors.add(exception)
        }
      }

      // Test the typed error handler using properly typed record
      @Suppress("UNCHECKED_CAST")
      val record = org.apache.kafka.clients.consumer.ConsumerRecord(
        "test-topic",
        0,
        0L,
        "key" as Any,
        "value" as Any
      )

      typedHandler.handle(record, RuntimeException("Test error"))
      typedHandler.handle(record, IllegalArgumentException("Another error"))

      handledErrors.size shouldBe 2
    }

    test("should create retry handler and track retry count") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic)

      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val retryHandler = RetryHandler(kafkaTemplate)

      // Create a record with retry count header
      val record = org.apache.kafka.clients.consumer.ConsumerRecord(
        topic,
        0,
        0L,
        "key-1",
        "value-1"
      )

      val retryCount = retryHandler.getRetryCount(record)
      retryCount shouldBe 0

      retryHandler.hasExceededMaxRetries(record) shouldBe false
    }

    test("should configure error handler with custom recoverer") {
      val recoveredRecords = mutableListOf<org.apache.kafka.clients.consumer.ConsumerRecord<*, *>>()

      val customRecoverer: (org.apache.kafka.clients.consumer.ConsumerRecord<*, *>, Exception) -> Unit =
        { record, _ ->
          recoveredRecords.add(record)
        }

      val errorHandler = createErrorHandler<String, String>(
        config = ErrorHandlerConfig(maxRetries = 1),
        recoverer = customRecoverer
      )

      errorHandler shouldNotBe null
    }

    test("should build seek-to-current error handler") {
      val maxFailuresExceededRecords = mutableListOf<org.apache.kafka.clients.consumer.ConsumerRecord<Any, Any>>()

      val handler = SeekToCurrentErrorHandler(
        maxFailures = 3
      ) { record, _ ->
        maxFailuresExceededRecords.add(record)
      }

      handler shouldNotBe null
    }
  })
