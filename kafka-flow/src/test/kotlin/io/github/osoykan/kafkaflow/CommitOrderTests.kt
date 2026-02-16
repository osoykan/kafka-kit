package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.CommitStrategy
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class CommitOrderTests :
  FunSpec({

    // ─────────────────────────────────────────────────────────────
    // OrderedCommitter Unit Tests
    // ─────────────────────────────────────────────────────────────

    test("OrderedCommitter with BySize(1) commits immediately when contiguous") {
      // BySize(1) is per-record commit - commits immediately when contiguous offsets found
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Register offsets in delivery order (simulates listener receiving records)
      (0L..4L).forEach { committer.registerOffset(0, it) }

      // Completions arrive out of order: 2, 0, 1, 4, 3
      val results = listOf(2L, 0L, 1L, 4L, 3L).map { offset ->
        committer.onComplete(
          CompletionEvent(
            partition = 0,
            offset = offset,
            acknowledge = { ackCalls.add(offset) }
          )
        )
      }

      // Results analysis (per-record commits, only highest contiguous offset gets ack'd):
      // After 2: completed={2}, lastCommitted=-1 -> can't commit (gap at 0,1)
      // After 0: completed={0,2}, lastCommitted=-1 -> commit 0, lastCommitted=0
      // After 1: completed={1,2}, lastCommitted=0 -> commit up to 2 (only ack 2), lastCommitted=2
      // After 4: completed={4}, lastCommitted=2 -> can't commit (gap at 3)
      // After 3: completed={3,4}, lastCommitted=2 -> commit up to 4 (only ack 4), lastCommitted=4

      results[0].isEmpty shouldBe true // Offset 2: waiting for 0,1
      results[1].commits[0] shouldBe 0L // Offset 0: commit 0
      results[2].commits[0] shouldBe 2L // Offset 1: commit up to 2
      results[3].isEmpty shouldBe true // Offset 4: waiting for 3
      results[4].commits[0] shouldBe 4L // Offset 3: commit up to 4

      // Only highest contiguous offsets were ack'd (optimization: single commit per batch)
      // In Kafka, committing offset N means "processed up to N" so we don't need to commit intermediates
      ackCalls shouldContainExactly listOf(0L, 2L, 4L)
    }

    test("OrderedCommitter with BySize(n) batches commits") {
      // BySize(3) commits when 3 records have completed
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(3))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Add 2 records - should not commit yet
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) }).isEmpty shouldBe true
      committer.onComplete(CompletionEvent(0, 1L) { ackCalls.add(1L) }).isEmpty shouldBe true

      ackCalls shouldHaveSize 0 // No commits yet

      // 3rd record triggers commit
      val result = committer.onComplete(CompletionEvent(0, 2L) { ackCalls.add(2L) })
      result.commits[0] shouldBe 2L

      // Only highest offset ack'd
      ackCalls shouldContainExactly listOf(2L)
    }

    test("OrderedCommitter handles multiple partitions independently") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      // Register offsets in delivery order
      committer.registerOffset(0, 0L)
      committer.registerOffset(0, 1L)
      committer.registerOffset(1, 0L)
      committer.registerOffset(1, 1L)
      committer.registerOffset(1, 2L)

      // Partition 0: offsets 1, 0 (out of order)
      committer.onComplete(CompletionEvent(0, 1L) {}).isEmpty shouldBe true
      committer.onComplete(CompletionEvent(0, 0L) {}).commits[0] shouldBe 1L

      // Partition 1: offsets 0, 2, 1 (out of order)
      committer.onComplete(CompletionEvent(1, 0L) {}).commits[1] shouldBe 0L
      committer.onComplete(CompletionEvent(1, 2L) {}).isEmpty shouldBe true
      committer.onComplete(CompletionEvent(1, 1L) {}).commits[1] shouldBe 2L

      val stats = committer.getStats()
      stats[0]?.lastCommitted shouldBe 1L // Partition 0 last committed: 1
      stats[1]?.lastCommitted shouldBe 2L // Partition 1 last committed: 2
    }

    test("OrderedCommitter tracks pending offsets in stats") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(100)) // Large batch to prevent auto-commit

      // Register starting offset
      committer.registerOffset(0, 0L)

      // Add some non-contiguous completions
      committer.onComplete(CompletionEvent(0, 2L) {})
      committer.onComplete(CompletionEvent(0, 4L) {})
      committer.onComplete(CompletionEvent(0, 5L) {})

      val stats = committer.getStats()[0]!!
      stats.lastCommitted shouldBe -1L // Nothing committed yet
      stats.pendingCount shouldBe 3
      stats.pendingOffsets shouldContainAll listOf(2L, 4L, 5L)
    }

    test("OrderedCommitter flush commits highest pending even with gaps") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(100)) // Large batch
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Add non-contiguous completions
      committer.onComplete(CompletionEvent(0, 2L) { ackCalls.add(2L) })
      committer.onComplete(CompletionEvent(0, 4L) { ackCalls.add(4L) })

      ackCalls shouldHaveSize 0 // Nothing committed yet

      // Flush commits the highest pending offset
      // (Kafka semantics: committing 4 means "processed up to 4")
      committer.flush()

      ackCalls shouldHaveSize 1
      ackCalls shouldContainExactly listOf(4L)

      val stats = committer.getStats()[0]!!
      stats.lastCommitted shouldBe 4L
      stats.pendingCount shouldBe 0
    }

    test("OrderedCommitter reset clears all state") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      committer.onComplete(CompletionEvent(0, 0L) {})
      committer.onComplete(CompletionEvent(1, 0L) {})

      committer.getStats().size shouldBe 2

      committer.reset()

      committer.getStats().size shouldBe 0
    }

    test("OrderedCommitter resetPartition clears specific partition") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      committer.onComplete(CompletionEvent(0, 0L) {})
      committer.onComplete(CompletionEvent(1, 0L) {})

      committer.resetPartition(0)

      committer.getStats().keys shouldContainExactly setOf(1)
    }

    test("OrderedCommitter onCommit callback is invoked for highest contiguous") {
      val commits = CopyOnWriteArrayList<Pair<Int, Long>>()
      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onCommit = { partition, offset -> commits.add(partition to offset) }
      )

      // Partition 0: offset 0, then 1 (contiguous)
      committer.onComplete(CompletionEvent(0, 0L) {}) // Commits 0
      committer.onComplete(CompletionEvent(0, 1L) {}) // Commits 1
      // Partition 1: offset 0
      committer.onComplete(CompletionEvent(1, 0L) {}) // Commits 0

      commits shouldContainExactly listOf(0 to 0L, 0 to 1L, 1 to 0L)
    }

    // ─────────────────────────────────────────────────────────────
    // OrderedCommitter Edge Case Tests
    // ─────────────────────────────────────────────────────────────

    test("ByTime strategy commits only after interval") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.ByTime(200.milliseconds))
      val ackCalls = CopyOnWriteArrayList<Long>()

      committer.start()

      // Add completions - should NOT commit immediately
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) })
      committer.onComplete(CompletionEvent(0, 1L) { ackCalls.add(1L) })
      committer.onComplete(CompletionEvent(0, 2L) { ackCalls.add(2L) })

      ackCalls shouldHaveSize 0 // No immediate commit for ByTime

      // Wait for timer to fire
      delay(300.milliseconds)

      // Now should have committed
      ackCalls shouldContainExactly listOf(2L) // Only highest contiguous

      committer.stop()
    }

    test("BySizeOrTime commits on count threshold before time") {
      val ackCalls = CopyOnWriteArrayList<Long>()
      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySizeOrTime(size = 3, interval = 10.seconds),
        onCommit = { _, offset -> ackCalls.add(offset) }
      )

      committer.start()

      // Give the commit manager time to start
      delay(50.milliseconds)

      // Add 2 completions - should not commit
      committer.onComplete(CompletionEvent(0, 0L) {})
      committer.onComplete(CompletionEvent(0, 1L) {})
      ackCalls shouldHaveSize 0

      // 3rd completion triggers commit via signal
      committer.onComplete(CompletionEvent(0, 2L) {})

      // Give signal time to be processed by whileSelect
      delay(200.milliseconds)

      ackCalls shouldContainExactly listOf(2L)

      committer.stop()
    }

    test("BySizeOrTime commits on time interval before count") {
      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySizeOrTime(size = 100, interval = 200.milliseconds)
      )
      val ackCalls = CopyOnWriteArrayList<Long>()

      committer.start()

      // Add fewer than size threshold
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) })
      committer.onComplete(CompletionEvent(0, 1L) { ackCalls.add(1L) })

      ackCalls shouldHaveSize 0

      // Wait for timer
      delay(300.milliseconds)

      ackCalls shouldContainExactly listOf(1L)

      committer.stop()
    }

    test("Empty batch commit returns null") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      // No completions added
      committer.getStats() shouldBe emptyMap()

      // Flush on empty should not throw
      committer.flush()
      committer.getStats() shouldBe emptyMap()
    }

    test("Double start is idempotent") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.ByTime(1.seconds))

      committer.start()
      committer.start() // Should not throw or create duplicate timers

      committer.stop()
    }

    test("Stop without start is safe") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.ByTime(1.seconds))

      committer.stop() // Should not throw
    }

    test("Large offset gaps are handled correctly") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(100))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Add offset 0 and 1000 - huge gap
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) })
      committer.onComplete(CompletionEvent(0, 1000L) { ackCalls.add(1000L) })

      // Only offset 0 is contiguous from -1
      val stats = committer.getStats()[0]!!
      stats.lastCommitted shouldBe -1L // Nothing committed yet (batch size not reached)
      stats.pendingCount shouldBe 2

      // Flush should commit highest
      committer.flush()
      ackCalls shouldContainExactly listOf(1000L)
    }

    test("Duplicate completion for same offset is handled") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Complete offset 0 twice
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) })
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) }) // Duplicate

      // First one committed, second is ignored (TreeSet dedupes)
      ackCalls shouldContainExactly listOf(0L)
    }

    // ─────────────────────────────────────────────────────────────
    // Gap Detection Tests
    // ─────────────────────────────────────────────────────────────

    test("onGapDetected is called when out-of-order completion creates a gap") {
      var gapDetected = false
      var gapClosed = false

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { gapDetected = true },
        onGapClosed = { gapClosed = true }
      )

      // Register starting offset so committer knows offset 0 is expected
      committer.registerOffset(0, 0L)

      // Complete offset 2 first (creates gap - offsets 0 and 1 are missing)
      committer.onComplete(CompletionEvent(0, 2L) {})

      gapDetected shouldBe true
      gapClosed shouldBe false
    }

    test("onGapClosed is called when gap-closing offset completes") {
      var gapDetected = false
      var gapClosed = false

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { gapDetected = true },
        onGapClosed = { gapClosed = true }
      )

      // Register starting offset
      committer.registerOffset(0, 0L)

      // Create gap: complete offset 2 first
      committer.onComplete(CompletionEvent(0, 2L) {})
      gapDetected shouldBe true
      gapClosed shouldBe false

      // Complete offset 1 - still a gap (offset 0 missing)
      committer.onComplete(CompletionEvent(0, 1L) {})
      gapClosed shouldBe false

      // Complete offset 0 - gap closed, commits 0, 1, 2
      committer.onComplete(CompletionEvent(0, 0L) {})
      gapClosed shouldBe true
    }

    test("no gap detected when completions arrive in order") {
      var gapDetected = false
      var gapClosed = false

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { gapDetected = true },
        onGapClosed = { gapClosed = true }
      )

      // Complete in order: 0, 1, 2
      committer.onComplete(CompletionEvent(0, 0L) {})
      committer.onComplete(CompletionEvent(0, 1L) {})
      committer.onComplete(CompletionEvent(0, 2L) {})

      gapDetected shouldBe false
      gapClosed shouldBe false // Never had a gap
    }

    test("gap detection works independently per partition") {
      val gapPartitions = CopyOnWriteArrayList<Int>()
      var closedCount = 0

      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onGapDetected = { /* Gaps are detected per-completion, tracked internally */ },
        onGapClosed = { closedCount++ }
      )

      // Register starting offsets for both partitions
      committer.registerOffset(0, 0L)
      committer.registerOffset(1, 0L)

      // Partition 0: create gap (offset 2 before 0, 1)
      committer.onComplete(CompletionEvent(0, 2L) {})

      // Partition 1: in order (no gap)
      committer.onComplete(CompletionEvent(1, 0L) {})
      committer.onComplete(CompletionEvent(1, 1L) {})

      // Partition 0: close gap
      committer.onComplete(CompletionEvent(0, 0L) {})
      committer.onComplete(CompletionEvent(0, 1L) {})

      closedCount shouldBe 1 // Gap closed once for partition 0
    }

    test("CommitResult includes hasRemainingGaps status") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      // Register starting offset
      committer.registerOffset(0, 0L)

      // Create gap: complete offset 3 first
      val result1 = committer.onComplete(CompletionEvent(0, 3L) {})
      result1.isEmpty shouldBe true // Can't commit

      // Complete offset 0 - can commit but gap remains (1, 2 missing)
      val result2 = committer.onComplete(CompletionEvent(0, 0L) {})
      result2.commits[0] shouldBe 0L
      result2.hasRemainingGaps shouldBe true // Still have pending 3

      // Complete 1, 2 - now gap is fully closed
      committer.onComplete(CompletionEvent(0, 1L) {})
      val result3 = committer.onComplete(CompletionEvent(0, 2L) {})
      result3.commits[0] shouldBe 3L
      result3.hasRemainingGaps shouldBe false
    }

    test("Concurrent completions are thread-safe") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1000)) // Large batch to prevent auto-commit
      val completionCount = AtomicInteger(0)

      // Spawn many coroutines adding completions concurrently
      val jobs = (0 until 100).map { i ->
        async(Dispatchers.Default) {
          committer.onComplete(
            CompletionEvent(0, i.toLong()) {
              completionCount.incrementAndGet()
            }
          )
        }
      }

      // Wait for all coroutines to complete
      jobs.forEach { it.await() }

      // All 100 completions should be recorded
      val stats = committer.getStats()[0]!!
      stats.pendingCount shouldBe 100

      // Flush should commit highest
      committer.flush()
      completionCount.get() shouldBe 1 // Only highest offset ack'd
    }

    test("Very high offset numbers work correctly") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

      val highOffset = Long.MAX_VALUE - 10

      // Without registerOffset, the first completion is treated as the starting offset
      committer.onComplete(CompletionEvent(0, highOffset) { ackCalls.add(highOffset) })

      // Should commit immediately -- this is the first and only known offset
      ackCalls shouldHaveSize 1
      ackCalls shouldContainExactly listOf(highOffset)
    }

    test("Reset during active timer stops cleanly") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.ByTime(100.milliseconds))

      committer.start()
      committer.onComplete(CompletionEvent(0, 0L) {})

      // Reset while timer is running
      committer.reset()

      committer.getStats() shouldBe emptyMap()
    }

    test("BySize with exact batch boundary commits correctly") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(3))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // First batch of 3
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) })
      committer.onComplete(CompletionEvent(0, 1L) { ackCalls.add(1L) })
      committer.onComplete(CompletionEvent(0, 2L) { ackCalls.add(2L) })

      ackCalls shouldContainExactly listOf(2L) // First batch committed

      // Second batch of 3
      committer.onComplete(CompletionEvent(0, 3L) { ackCalls.add(3L) })
      committer.onComplete(CompletionEvent(0, 4L) { ackCalls.add(4L) })
      committer.onComplete(CompletionEvent(0, 5L) { ackCalls.add(5L) })

      ackCalls shouldContainExactly listOf(2L, 5L) // Both batches committed
    }

    test("Partial batch followed by flush") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(10))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Add only 5 records (less than batch size)
      repeat(5) { i ->
        committer.onComplete(CompletionEvent(0, i.toLong()) { ackCalls.add(i.toLong()) })
      }

      ackCalls shouldHaveSize 0 // No commit yet

      // Flush commits partial batch
      committer.flush()
      ackCalls shouldContainExactly listOf(4L)
    }
  })
