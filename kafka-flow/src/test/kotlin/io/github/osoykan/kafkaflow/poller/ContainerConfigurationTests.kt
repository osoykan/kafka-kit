package io.github.osoykan.kafkaflow.poller

import io.github.osoykan.kafkaflow.ListenerConfig
import io.github.osoykan.kafkaflow.TopicConfig
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class ContainerConfigurationTests :
  FunSpec({

    test("ContainerConfiguration should apply topic-specific groupId overrides") {
      val containerProperties = ContainerConfiguration.createContainerProperties(
        topicConfig = TopicConfig(topics = listOf("orders"), groupId = "orders-group"),
        listenerConfig = ListenerConfig()
      )

      containerProperties.groupId shouldBe "orders-group"
    }
  })
