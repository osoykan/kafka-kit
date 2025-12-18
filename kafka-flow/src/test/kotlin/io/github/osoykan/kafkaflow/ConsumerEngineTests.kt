package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class ConsumerEngineTests :
  FunSpec({

    test("ConsumerEngine should skip start when disabled") {
      val engine = ConsumerEngine<String, String>(
        consumers = emptyList(),
        supervisorFactory = NoOpSupervisorFactory(),
        enabled = false
      )

      engine.start()

      engine.isStarted() shouldBe false
      engine.activeSupervisorCount() shouldBe 0
    }

    test("ConsumerEngine should be idempotent on double start") {
      var createCount = 0
      val factory = object : ConsumerSupervisorFactory<String, String> {
        override fun createSupervisors(consumers: List<Consumer<String, String>>): List<ConsumerSupervisor> {
          createCount++
          return emptyList()
        }
      }

      val engine = ConsumerEngine<String, String>(
        consumers = emptyList(),
        supervisorFactory = factory,
        enabled = true
      )

      engine.start()
      engine.start() // Second call should be ignored

      createCount shouldBe 1
      engine.isStarted() shouldBe true
    }

    test("ConsumerEngine should be idempotent on double stop") {
      val engine = ConsumerEngine<String, String>(
        consumers = emptyList(),
        supervisorFactory = NoOpSupervisorFactory(),
        enabled = true
      )

      engine.start()
      engine.stop()
      engine.stop() // Second call should be safe

      engine.isStarted() shouldBe false
    }

    test("ConsumerEngine stop without start should be safe") {
      val engine = ConsumerEngine<String, String>(
        consumers = emptyList(),
        supervisorFactory = NoOpSupervisorFactory(),
        enabled = true
      )

      engine.stop() // Should not throw
      engine.isStarted() shouldBe false
    }

    test("ConsumerEngine consumerNames should return supervisor names") {
      val mockSupervisor = object : ConsumerSupervisor {
        override val consumerName = "TestConsumer"
        override val topics = listOf("test-topic")

        override fun start() {}

        override fun stop() {}

        override fun isRunning() = true
      }

      val factory = object : ConsumerSupervisorFactory<String, String> {
        override fun createSupervisors(consumers: List<Consumer<String, String>>) = listOf(mockSupervisor)
      }

      val engine = ConsumerEngine<String, String>(
        consumers = emptyList(),
        supervisorFactory = factory,
        enabled = true
      )

      engine.start()

      engine.consumerNames() shouldBe listOf("TestConsumer")
      engine.activeSupervisorCount() shouldBe 1
    }
  })

private class NoOpSupervisorFactory<K : Any, V : Any> : ConsumerSupervisorFactory<K, V> {
  override fun createSupervisors(consumers: List<Consumer<K, V>>): List<ConsumerSupervisor> = emptyList()
}
