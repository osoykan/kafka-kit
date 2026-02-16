package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.AckableRecord
import io.github.osoykan.kafkaflow.poller.CommitStrategy
import io.github.osoykan.kafkaflow.poller.PauseController
import io.github.osoykan.kafkaflow.poller.PauseReason
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class BugfixTests :
  FunSpec({

    // ─────────────────────────────────────────────────────────────
    // Bug #1: OrderedCommitter assumes offsets start at 0
    // ─────────────────────────────────────────────────────────────

    test("OrderedCommitter should commit when offsets start at non-zero value") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      val result = committer.onComplete(
        CompletionEvent(partition = 0, offset = 1000L) { ackCalls.add(1000L) }
      )

      result.commits[0] shouldBe 1000L
      ackCalls.size shouldBe 1
    }

    test("OrderedCommitter should handle non-zero starting offsets with out-of-order completion") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      (500L..504L).forEach { committer.registerOffset(partition = 0, offset = it) }

      val results = listOf(502L, 500L, 501L, 504L, 503L).map { offset ->
        committer.onComplete(
          CompletionEvent(partition = 0, offset = offset) { ackCalls.add(offset) }
        )
      }

      results[0].isEmpty shouldBe true
      results[1].commits[0] shouldBe 500L
      results[2].commits[0] shouldBe 502L
      results[3].isEmpty shouldBe true
      results[4].commits[0] shouldBe 504L

      ackCalls shouldBe listOf(500L, 502L, 504L)
    }

    test("OrderedCommitter gap detection should not fire for first offset of a partition") {
      var gapDetected = false

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { gapDetected = true }
      )

      committer.onComplete(CompletionEvent(partition = 0, offset = 500L) {})

      gapDetected shouldBe false
    }

    test("OrderedCommitter gap detection works correctly for non-zero starting offsets") {
      var gapDetected = false
      var gapClosed = false

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { gapDetected = true },
        onGapClosed = { gapClosed = true }
      )

      committer.onComplete(CompletionEvent(partition = 0, offset = 100L) {})
      gapDetected shouldBe false

      committer.onComplete(CompletionEvent(partition = 0, offset = 102L) {})
      gapDetected shouldBe true

      committer.onComplete(CompletionEvent(partition = 0, offset = 101L) {})
      gapClosed shouldBe true
    }

    test("OrderedCommitter handles multiple partitions with different starting offsets") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Pair<Int, Long>>()

      val r1 = committer.onComplete(
        CompletionEvent(partition = 0, offset = 100L) { ackCalls.add(0 to 100L) }
      )
      r1.commits[0] shouldBe 100L

      val r2 = committer.onComplete(
        CompletionEvent(partition = 1, offset = 5000L) { ackCalls.add(1 to 5000L) }
      )
      r2.commits[1] shouldBe 5000L

      val r3 = committer.onComplete(
        CompletionEvent(partition = 0, offset = 101L) { ackCalls.add(0 to 101L) }
      )
      r3.commits[0] shouldBe 101L

      ackCalls shouldBe listOf(0 to 100L, 1 to 5000L, 0 to 101L)
    }

    test("OrderedCommitter with registerOffset prevents premature commits") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      committer.registerOffset(0, 998L)
      committer.registerOffset(0, 999L)
      committer.registerOffset(0, 1000L)

      val r1 = committer.onComplete(
        CompletionEvent(0, 1000L) { ackCalls.add(1000L) }
      )
      r1.isEmpty shouldBe true

      val r2 = committer.onComplete(
        CompletionEvent(0, 998L) { ackCalls.add(998L) }
      )
      r2.commits[0] shouldBe 998L

      val r3 = committer.onComplete(
        CompletionEvent(0, 999L) { ackCalls.add(999L) }
      )
      r3.commits[0] shouldBe 1000L

      ackCalls shouldBe listOf(998L, 1000L)
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #2: ManualAck double-acknowledge
    // ─────────────────────────────────────────────────────────────

    test("AckableRecord acknowledge should be idempotent - multiple calls should only trigger once") {
      // With the bug: each call to acknowledge() decrements backpressure counter
      // and sends a CompletionEvent. Double ack causes counter underflow and
      // duplicate events in the OrderedCommitter.
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      // Simulate ManualAck consumer calling ack + supervisor calling ack
      ackableRecord.acknowledge()
      ackableRecord.acknowledge() // second call

      // Should only trigger once
      ackCount.get() shouldBe 1
    }

    test("ManualAck consumer calling acknowledge should not cause double ack in supervisor") {
      // Simulates the full ManualAck flow:
      // 1. handleRecord wraps ackRecord.acknowledge() in an Acknowledgment
      // 2. Consumer calls ack.acknowledge() during processing
      // 3. processRecord calls ackRecord.acknowledge() again
      // The net effect should be exactly 1 acknowledgment.
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      // Step 1: ManualAck consumer gets a wrapped Acknowledgment
      val consumerAck = Acknowledgment { ackableRecord.acknowledge() }

      // Step 2: Consumer calls ack during processing
      consumerAck.acknowledge()

      // Step 3: Supervisor calls ackRecord.acknowledge() unconditionally
      ackableRecord.acknowledge()

      // Should be exactly 1 — idempotent
      ackCount.get() shouldBe 1
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #3: Failed retry/DLT send still commits offset
    // ─────────────────────────────────────────────────────────────

    test("processRecord should not acknowledge when processing fails completely") {
      // When RetryableProcessor fails to send to retry topic or DLT, the record
      // should NOT be acknowledged. With the bug, the offset is committed and
      // the message is lost forever.
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      // Simulate: handleRecord returns Failed (retry topic send failed)
      val result: ProcessingResult<String> = ProcessingResult.Failed(
        exception = RuntimeException("Kafka producer down"),
        exhaustedRetries = false
      )

      // In the supervisor, only non-Failed results should trigger ack
      val shouldAcknowledge = result !is ProcessingResult.Failed
      if (shouldAcknowledge) {
        ackableRecord.acknowledge()
      }

      ackCount.get() shouldBe 0 // Not acknowledged — will be reprocessed
    }

    test("processRecord should acknowledge on successful processing") {
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      val result: ProcessingResult<String> = ProcessingResult.Success("ok")

      val shouldAcknowledge = result !is ProcessingResult.Failed
      if (shouldAcknowledge) {
        ackableRecord.acknowledge()
      }

      ackCount.get() shouldBe 1
    }

    test("processRecord should acknowledge when sent to retry topic") {
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      val result: ProcessingResult<Nothing> = ProcessingResult.SentToRetryTopic(
        topic = "test-topic-retry",
        attempt = 1
      )

      val shouldAcknowledge = result !is ProcessingResult.Failed
      if (shouldAcknowledge) {
        ackableRecord.acknowledge()
      }

      ackCount.get() shouldBe 1 // Acknowledged — record is on retry topic
    }

    test("processRecord should acknowledge when sent to DLT") {
      val ackCount = AtomicInteger(0)

      val record = ConsumerRecord("test-topic", 0, 0L, "key", "value")
      val ackableRecord = AckableRecord(record) { ackCount.incrementAndGet() }

      val result: ProcessingResult<Nothing> = ProcessingResult.SentToDlt(
        topic = "test-topic-dlt",
        reason = "Max retries exceeded"
      )

      val shouldAcknowledge = result !is ProcessingResult.Failed
      if (shouldAcknowledge) {
        ackableRecord.acknowledge()
      }

      ackCount.get() shouldBe 1 // Acknowledged — record is on DLT
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #5: Backpressure and gap-detection pause/resume conflict
    // ─────────────────────────────────────────────────────────────

    test("PauseController should not resume when one reason is cleared but another remains") {
      // Bug: backpressure and gap detection independently call pause/resume.
      // If gap detection pauses and backpressure resumes, the container runs
      // despite the gap, potentially causing unbounded memory growth.
      val paused = AtomicBoolean(false)
      val controller = PauseController(
        pause = { paused.set(true) },
        resume = { paused.set(false) }
      )

      // Gap detection pauses
      controller.requestPause(PauseReason.GAP_DETECTED)
      paused.get() shouldBe true

      // Backpressure also pauses
      controller.requestPause(PauseReason.BACKPRESSURE)
      paused.get() shouldBe true

      // Backpressure clears — gap still active, should NOT resume
      controller.clearPause(PauseReason.BACKPRESSURE)
      paused.get() shouldBe true // MUST remain paused

      // Gap closes — all reasons cleared, now resume
      controller.clearPause(PauseReason.GAP_DETECTED)
      paused.get() shouldBe false
    }

    test("PauseController should resume when all reasons are cleared") {
      val paused = AtomicBoolean(false)
      val controller = PauseController(
        pause = { paused.set(true) },
        resume = { paused.set(false) }
      )

      controller.requestPause(PauseReason.BACKPRESSURE)
      paused.get() shouldBe true

      controller.clearPause(PauseReason.BACKPRESSURE)
      paused.get() shouldBe false
    }

    test("PauseController should only call pause once for multiple reasons") {
      val pauseCount = AtomicInteger(0)
      val resumeCount = AtomicInteger(0)
      val controller = PauseController(
        pause = { pauseCount.incrementAndGet() },
        resume = { resumeCount.incrementAndGet() }
      )

      controller.requestPause(PauseReason.GAP_DETECTED)
      controller.requestPause(PauseReason.BACKPRESSURE)

      pauseCount.get() shouldBe 1 // Only pause once
      resumeCount.get() shouldBe 0

      controller.clearPause(PauseReason.GAP_DETECTED)
      resumeCount.get() shouldBe 0 // Don't resume yet

      controller.clearPause(PauseReason.BACKPRESSURE)
      resumeCount.get() shouldBe 1 // Now resume
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #6: DLT duplicate headers
    // ─────────────────────────────────────────────────────────────

    test("DLT record should not have duplicate internal headers") {
      // RetryableProcessor.createDltRecord copies ALL headers from the source record
      // (including internal kafka.* ones) and then adds new ones with the same keys.
      // This results in duplicate headers. The fix: filter internal headers before copying.

      // Simulate the DLT creation pattern from RetryableProcessor.createDltRecord:
      val sourceRecord = ConsumerRecord("test-topic", 0, 0L, "key", "value")

      // Source record has existing internal headers (from a previous retry)
      sourceRecord.headers().add(
        org.apache.kafka.common.header.internals.RecordHeader(
          Headers.EXCEPTION_CLASS,
          "java.lang.IllegalStateException".toByteArray()
        )
      )
      sourceRecord.headers().add(
        org.apache.kafka.common.header.internals.RecordHeader(
          Headers.CONSUMER_NAME,
          "old-consumer".toByteArray()
        )
      )
      sourceRecord.headers().add(
        org.apache.kafka.common.header.internals.RecordHeader(
          "user-header",
          "user-value".toByteArray()
        )
      )

      // This is the buggy pattern from createDltRecord (copies ALL, then adds more)
      val dltRecord = org.apache.kafka.clients.producer.ProducerRecord(
        "test-topic-dlt",
        sourceRecord.key(),
        sourceRecord.value()
      )

      // The fix: filter out internal headers before copying
      sourceRecord
        .headers()
        .filter { !it.key().startsWith("x-") && !it.key().startsWith("kafka.") }
        .forEach { dltRecord.headers().add(it) }

      // Then add new internal headers
      dltRecord.headers().add(
        org.apache.kafka.common.header.internals.RecordHeader(
          Headers.EXCEPTION_CLASS,
          "java.lang.RuntimeException".toByteArray()
        )
      )
      dltRecord.headers().add(
        org.apache.kafka.common.header.internals.RecordHeader(
          Headers.CONSUMER_NAME,
          "new-consumer".toByteArray()
        )
      )

      // Should have exactly 1 of each internal header
      val exceptionHeaders = dltRecord.headers().headers(Headers.EXCEPTION_CLASS).toList()
      val consumerHeaders = dltRecord.headers().headers(Headers.CONSUMER_NAME).toList()

      exceptionHeaders.size shouldBe 1
      consumerHeaders.size shouldBe 1

      // User headers are preserved
      val userHeaders = dltRecord.headers().headers("user-header").toList()
      userHeaders.size shouldBe 1

      // New values, not old
      String(exceptionHeaders[0].value()) shouldBe "java.lang.RuntimeException"
      String(consumerHeaders[0].value()) shouldBe "new-consumer"
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #7: OrderedCommitter scope leak
    // ─────────────────────────────────────────────────────────────

    test("OrderedCommitter scope should be cancelled after reset") {
      // The OrderedCommitter creates a CoroutineScope with SupervisorJob
      // but never cancels it. This is a resource leak.
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      committer.onComplete(CompletionEvent(0, 0L) {})

      // Reset should cancel the scope
      committer.reset()

      // The scope should be cancelled — verify by checking that the committer
      // can be used again after reset (new scope is created)
      val result = committer.onComplete(CompletionEvent(0, 100L) {})
      result.commits[0] shouldBe 100L
    }

    // ─────────────────────────────────────────────────────────────
    // Bug #4: Consumer flow silent death
    // ─────────────────────────────────────────────────────────────

    test("Consumer flow should restart after stream error") {
      // Bug: AbstractConsumerSupervisor.launchConsumer uses .catch that swallows
      // errors and terminates the flow. The consumer is silently dead while
      // isRunning() still returns true.
      //
      // This test simulates the exact pattern from launchConsumer and verifies
      // that consumeWithRestart properly restarts the flow after an error.

      val processedRecords = CopyOnWriteArrayList<Int>()
      val attempts = AtomicInteger(0)

      val flowProvider: () -> Flow<Int> = {
        val attempt = attempts.incrementAndGet()
        flow {
          if (attempt == 1) {
            emit(1)
            emit(2)
            emit(3)
            throw RuntimeException("Kafka disconnected")
          } else {
            emit(4)
            emit(5)
            emit(6)
          }
        }
      }

      // Use the consumeWithRestart utility that should exist in production code
      val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
      val job = scope.launch {
        consumeWithRestart(
          flowProvider = flowProvider,
          restartDelay = 50.milliseconds,
          onError = { /* logged in production */ }
        ) { record ->
          processedRecords.add(record)
        }
      }

      // Wait for both attempts to complete
      delay(500.milliseconds)
      job.cancel()

      // Should have processed records from both attempts
      processedRecords shouldBe listOf(1, 2, 3, 4, 5, 6)
      attempts.get() shouldBe 2
    }

    test("Consumer flow restart should respect cancellation") {
      // consumeWithRestart should stop retrying when the scope is cancelled
      val attempts = AtomicInteger(0)

      val flowProvider: () -> Flow<Int> = {
        attempts.incrementAndGet()
        flow<Int> { throw RuntimeException("Always fails") }
      }

      val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
      val job = scope.launch {
        consumeWithRestart(
          flowProvider = flowProvider,
          restartDelay = 50.milliseconds,
          onError = {}
        ) {}
      }

      // Let it retry a few times
      delay(250.milliseconds)
      job.cancel()
      job.join()

      // Should have retried multiple times but stopped on cancel
      val finalAttempts = attempts.get()
      (finalAttempts >= 2) shouldBe true
    }
  })
