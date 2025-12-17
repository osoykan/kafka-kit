package io.github.osoykan.ktor.springkafka

import io.ktor.server.application.*
import org.springframework.kafka.core.KafkaTemplate

/**
 * Get the default KafkaTemplate from the Spring Kafka plugin.
 *
 * ## Usage
 *
 * ```kotlin
 * fun Application.configureRouting() {
 *   val template = kafkaTemplate<String, MyEvent>()
 *
 *   routing {
 *     post("/send") {
 *       template.send("my-topic", "key", event).get()
 *     }
 *   }
 * }
 * ```
 *
 * @return Default KafkaTemplate for producing messages
 * @throws IllegalStateException if SpringKafka plugin is not installed
 */
inline fun <reified K : Any, reified V : Any> Application.kafkaTemplate(): KafkaTemplate<K, V> =
  kafkaTemplate(null)

/**
 * Get a named KafkaTemplate from the Spring Kafka plugin.
 *
 * Use this when you have configured multiple producer factories for different clusters.
 *
 * ## Usage
 *
 * ```kotlin
 * // Configure multiple producers
 * install(SpringKafka) {
 *   bootstrapServers = "cluster-a:9092"
 *
 *   producerFactory("analytics") {
 *     bootstrapServers = "analytics-cluster:9092"
 *   }
 * }
 *
 * // Use named template
 * fun Application.configureRouting() {
 *   val defaultTemplate = kafkaTemplate<String, Order>()
 *   val analyticsTemplate = kafkaTemplate<String, AnalyticsEvent>("analytics")
 *
 *   routing {
 *     post("/order") {
 *       defaultTemplate.send("orders", order).get()
 *       analyticsTemplate.send("analytics.orders", event).get()
 *     }
 *   }
 * }
 * ```
 *
 * @param name The producer factory name (as configured in `producerFactory("name") { }`)
 *             Pass `null` for the default template.
 * @return KafkaTemplate for producing messages
 * @throws IllegalStateException if SpringKafka plugin is not installed
 * @throws org.springframework.beans.factory.NoSuchBeanDefinitionException if named template doesn't exist
 */
inline fun <reified K : Any, reified V : Any> Application.kafkaTemplate(name: String?): KafkaTemplate<K, V> {
  val context = attributes.getOrNull(SpringKafkaContextKey)
    ?: throw IllegalStateException("SpringKafka plugin is not installed. Call install(SpringKafka) { ... } first.")

  val beanName = if (name.isNullOrBlank()) "kafkaTemplate" else "${name}KafkaTemplate"

  @Suppress("UNCHECKED_CAST")
  return context.getBean(beanName) as KafkaTemplate<K, V>
}

/**
 * Check if Spring Kafka is running.
 *
 * ```kotlin
 * if (application.isSpringKafkaRunning()) {
 *   // Kafka is ready
 * }
 * ```
 *
 * @return true if Spring Kafka context is running
 */
fun Application.isSpringKafkaRunning(): Boolean =
  attributes.getOrNull(SpringKafkaContextKey)?.isRunning() ?: false

/**
 * Check if a named KafkaTemplate exists.
 *
 * ```kotlin
 * if (application.hasKafkaTemplate("analytics")) {
 *   val template = application.kafkaTemplate<String, Event>("analytics")
 * }
 * ```
 *
 * @param name The producer factory name, or null for default template
 * @return true if the template exists
 */
fun Application.hasKafkaTemplate(name: String? = null): Boolean {
  val context = attributes.getOrNull(SpringKafkaContextKey) ?: return false
  val beanName = if (name.isNullOrBlank()) "kafkaTemplate" else "${name}KafkaTemplate"
  return context.containsBean(beanName)
}

/**
 * Check if a named listener container factory exists.
 *
 * ```kotlin
 * if (application.hasListenerContainerFactory("clusterB")) {
 *   // clusterBKafkaListenerContainerFactory is available
 * }
 * ```
 *
 * @param name The consumer factory name, or null for default factory
 * @return true if the factory exists
 */
fun Application.hasListenerContainerFactory(name: String? = null): Boolean {
  val context = attributes.getOrNull(SpringKafkaContextKey) ?: return false
  val beanName = if (name.isNullOrBlank()) "kafkaListenerContainerFactory" else "${name}KafkaListenerContainerFactory"
  return context.containsBean(beanName)
}

/**
 * Get a bean from the Spring Kafka context.
 *
 * Useful for accessing other beans registered in the Spring context.
 *
 * ```kotlin
 * val myBean = application.springKafkaBean<MyService>()
 * ```
 *
 * @return The bean instance
 * @throws IllegalStateException if SpringKafka plugin is not installed
 */
inline fun <reified T : Any> Application.springKafkaBean(): T {
  val context = attributes.getOrNull(SpringKafkaContextKey)
    ?: throw IllegalStateException("SpringKafka plugin is not installed. Call install(SpringKafka) { ... } first.")
  return context.getBean(T::class.java)
}

/**
 * Get a bean from the Spring Kafka context by class.
 *
 * ```kotlin
 * val myBean = application.springKafkaBean(MyService::class.java)
 * ```
 *
 * @return The bean instance
 * @throws IllegalStateException if SpringKafka plugin is not installed
 */
fun <T : Any> Application.springKafkaBean(clazz: Class<T>): T {
  val context = attributes.getOrNull(SpringKafkaContextKey)
    ?: throw IllegalStateException("SpringKafka plugin is not installed. Call install(SpringKafka) { ... } first.")
  return context.getBean(clazz)
}

/**
 * Get a named bean from the Spring Kafka context.
 *
 * ```kotlin
 * val factory = application.springKafkaBean("clusterBConsumerFactory", ConsumerFactory::class.java)
 * ```
 *
 * @param name The bean name
 * @param clazz The bean class
 * @return The bean instance
 * @throws IllegalStateException if SpringKafka plugin is not installed
 */
fun <T : Any> Application.springKafkaBean(name: String, clazz: Class<T>): T {
  val context = attributes.getOrNull(SpringKafkaContextKey)
    ?: throw IllegalStateException("SpringKafka plugin is not installed. Call install(SpringKafka) { ... } first.")
  return context.getBean(name, clazz)
}
