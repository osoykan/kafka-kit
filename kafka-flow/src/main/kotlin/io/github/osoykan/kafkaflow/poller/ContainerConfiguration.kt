package io.github.osoykan.kafkaflow.poller

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.*
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Shared container configuration used by both [SpringKafkaPoller] and
 * [AbstractBatchConsumerSupervisor][io.github.osoykan.kafkaflow.AbstractBatchConsumerSupervisor].
 *
 * Centralizes [ContainerProperties] creation, virtual-thread configuration,
 * and [ConcurrentMessageListenerContainer] instantiation so both per-record
 * and batch consumers share the same defaults.
 */
internal object ContainerConfiguration {
  fun createContainerProperties(
    topicConfig: TopicConfig,
    listenerConfig: ListenerConfig
  ): ContainerProperties =
    ContainerProperties(*topicConfig.topics.toTypedArray()).apply {
      pollTimeout = topicConfig.effectivePollTimeout(listenerConfig.pollTimeout).inWholeMilliseconds
      topicConfig.groupId?.let { setGroupId(it) }
      ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
      idleBetweenPolls = listenerConfig.idleBetweenPolls.inWholeMilliseconds
      isSyncCommits = true
      syncCommitTimeout = Duration.ofSeconds(5)
      configureVirtualThreads(this, topicConfig)
    }

  fun <K : Any, V : Any> createContainer(
    consumerFactory: ConsumerFactory<K, V>,
    containerProps: ContainerProperties,
    topicConfig: TopicConfig,
    listenerConfig: ListenerConfig,
    errorHandler: CommonErrorHandler? = null
  ): ConcurrentMessageListenerContainer<K, V> =
    ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
      concurrency = topicConfig.effectiveMultiplePartitions(listenerConfig.multiplePartitions)
      errorHandler?.let { commonErrorHandler = it }
    }

  private fun configureVirtualThreads(props: ContainerProperties, topicConfig: TopicConfig) {
    val executor = SimpleAsyncTaskExecutor("kafka-vt-").apply { setVirtualThreads(true) }
    props.listenerTaskExecutor = executor
    logger.debug { "ContainerConfiguration: Using virtual threads for topics: [${topicConfig.displayName}]" }
  }
}

/**
 * Thread-safe holder for late-initialized container reference.
 * Used to allow backpressure and gap detection callbacks to pause/resume the container.
 */
internal class ContainerRef<K : Any, V : Any> {
  @Volatile
  private lateinit var container: ConcurrentMessageListenerContainer<K, V>

  fun set(c: ConcurrentMessageListenerContainer<K, V>) {
    container = c
  }

  fun get(): ConcurrentMessageListenerContainer<K, V> = container
}
