package io.github.osoykan.kafkaflow

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

// Extension functions for working with Kafka records and flows.

/**
 * Gets a header value as a string.
 */
fun ConsumerRecord<*, *>.headerAsString(key: String): String? =
  headers().lastHeader(key)?.value()?.let { String(it) }

/**
 * Gets a header value as bytes.
 */
fun ConsumerRecord<*, *>.headerAsBytes(key: String): ByteArray? =
  headers().lastHeader(key)?.value()

/**
 * Gets all headers as a map.
 */
fun ConsumerRecord<*, *>.headersAsMap(): Map<String, ByteArray> =
  headers().associate { it.key() to it.value() }

/**
 * Checks if a record has a specific header.
 */
fun ConsumerRecord<*, *>.hasHeader(key: String): Boolean =
  headers().lastHeader(key) != null

/**
 * Maps only the value of a consumer record.
 */
fun <K, V, R> Flow<ConsumerRecord<K, V>>.mapValue(transform: (V) -> R): Flow<R> =
  map { transform(it.value()) }

/**
 * Maps the record to a pair of key and value.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.toKeyValuePairs(): Flow<Pair<K, V>> =
  map { it.key() to it.value() }

/**
 * Filters records by topic.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.filterByTopic(topic: String): Flow<ConsumerRecord<K, V>> =
  filter { it.topic() == topic }

/**
 * Filters records by partition.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.filterByPartition(partition: Int): Flow<ConsumerRecord<K, V>> =
  filter { it.partition() == partition }

/**
 * Filters records by key.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.filterByKey(key: K): Flow<ConsumerRecord<K, V>> =
  filter { it.key() == key }

/**
 * Filters records that have a specific header.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.filterByHeader(
  headerKey: String,
  headerValue: String
): Flow<ConsumerRecord<K, V>> =
  filter { it.headerAsString(headerKey) == headerValue }

/**
 * Logs each record for debugging.
 */
fun <K, V> Flow<ConsumerRecord<K, V>>.logRecords(
  prefix: String = ""
): Flow<ConsumerRecord<K, V>> = onEach { record ->
  println("$prefix[${record.topic()}:${record.partition()}:${record.offset()}] key=${record.key()}, value=${record.value()}")
}

/**
 * Extracts metadata from a consumer record.
 */
data class RecordMetadataInfo(
  val topic: String,
  val partition: Int,
  val offset: Long,
  val timestamp: Long,
  val timestampType: String,
  val serializedKeySize: Int,
  val serializedValueSize: Int,
  val headers: Map<String, ByteArray>
)

/**
 * Gets metadata information from a record.
 */
fun <K, V> ConsumerRecord<K, V>.metadataInfo(): RecordMetadataInfo = RecordMetadataInfo(
  topic = topic(),
  partition = partition(),
  offset = offset(),
  timestamp = timestamp(),
  timestampType = timestampType().name,
  serializedKeySize = serializedKeySize(),
  serializedValueSize = serializedValueSize(),
  headers = headersAsMap()
)

/**
 * Converts Duration to millis safely for Kafka configs.
 */
fun kotlin.time.Duration.toKafkaMillis(): Long = inWholeMilliseconds

/**
 * Converts Duration to seconds safely for Kafka configs.
 */
fun kotlin.time.Duration.toKafkaSeconds(): Int = inWholeSeconds.toInt()

// ─────────────────────────────────────────────────────────────
// Non-blocking Future extensions for suspend functions
// ─────────────────────────────────────────────────────────────

/**
 * Awaits a CompletableFuture without blocking.
 *
 * This is the primary extension for non-blocking Kafka publishing.
 * Use this instead of .get() to avoid blocking the coroutine.
 *
 * Example:
 * ```kotlin
 * val result = kafkaTemplate.send(record).await()
 * ```
 */
suspend fun <T> CompletableFuture<T>.await(): T = suspendCancellableCoroutine { cont ->
  whenComplete { result, exception ->
    if (exception != null) {
      cont.resumeWithException(exception)
    } else {
      cont.resume(result)
    }
  }

  cont.invokeOnCancellation {
    cancel(false)
  }
}

/**
 * Awaits a CompletableFuture without blocking, returning null on failure.
 *
 * Use this when you want to handle failures gracefully without exceptions.
 */
suspend fun <T> CompletableFuture<T>.awaitOrNull(): T? = try {
  await()
} catch (_: Throwable) {
  null
}

/**
 * Awaits a CompletableFuture without blocking, returning a Result.
 *
 * Use this when you want to handle both success and failure cases explicitly.
 */
suspend fun <T> CompletableFuture<T>.awaitResult(): Result<T> = try {
  Result.success(await())
} catch (e: Throwable) {
  Result.failure(e)
}
