package io.github.osoykan.kafkaflow.poller

import io.github.osoykan.kafkaflow.BackpressureConfig
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.concurrent.atomic.AtomicBoolean

class BackpressureControllerTests :
  FunSpec({

    test("BackpressureController should not treat Channel.BUFFERED as a negative buffer size") {
      val paused = AtomicBoolean(false)
      val controller = BackpressureController(
        pauseController = PauseController(
          pause = { paused.set(true) },
          resume = { paused.set(false) },
          topicName = "orders"
        ),
        config = BackpressureConfig(),
        bufferCapacity = DEFAULT_BUFFER_CAPACITY,
        topicName = "orders"
      )

      controller.onBufferAdd()

      paused.get() shouldBe false
    }
  })
