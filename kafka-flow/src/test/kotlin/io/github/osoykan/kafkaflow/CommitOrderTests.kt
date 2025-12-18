package io.github.osoykan.kafkaflow

import io.github.osoykan.kafkaflow.poller.AckableRecord
import io.github.osoykan.kafkaflow.poller.CommitStrategy
import io.github.osoykan.kafkaflow.support.SharedKafka
import io.github.osoykan.kafkaflow.support.TestHelpers
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class CommitOrderTests :
  FunSpec({
    val kafka = SharedKafka.instance

    // ─────────────────────────────────────────────────────────────
    // Basic Acknowledgment Tests
    // ─────────────────────────────────────────────────────────────

    test("acknowledgments should be called for all consumed records") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val acknowledgedValues = CopyOnWriteArrayList<String>()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer
          .consume(topicConfig)
          .take(5)
          .collect { ackRecord: AckableRecord<String, String> ->
            acknowledgedValues.add(ackRecord.record.value())
            ackRecord.acknowledge()
          }
      }

      delay(1.seconds)

      repeat(5) { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      job.await()
      consumer.stop()

      acknowledgedValues shouldHaveSize 5
      acknowledgedValues shouldContainAll listOf("value-0", "value-1", "value-2", "value-3", "value-4")
    }

    test("records consumed with sequential processing should preserve order") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val processedOrder = CopyOnWriteArrayList<Int>()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer
          .consume(topicConfig)
          .take(10)
          .collect { ackRecord ->
            val index = ackRecord.record.value().toInt()
            processedOrder.add(index)
            ackRecord.acknowledge()
          }
      }

      delay(1.seconds)

      repeat(10) { i ->
        kafkaTemplate.send(topic, "key-$i", i.toString()).get()
      }

      job.await()
      consumer.stop()

      processedOrder.size shouldBe 10
      processedOrder shouldContainExactly (0..9).toList()
    }

    // ─────────────────────────────────────────────────────────────
    // CommitStrategy Configuration Tests
    // ─────────────────────────────────────────────────────────────

    test("CommitStrategy.BySize(1) should commit per record") {
      val strategy = CommitStrategy.BySize(1)
      strategy.size shouldBe 1
      strategy.syncCommits shouldBe true
    }

    test("CommitStrategy.BySize should have correct batch size") {
      val strategy = CommitStrategy.BySize(100)
      strategy.size shouldBe 100
    }

    test("CommitStrategy.ByTime should have correct interval") {
      val strategy = CommitStrategy.ByTime(5.seconds)
      strategy.interval shouldBe 5.seconds
    }

    test("CommitStrategy.BySizeOrTime should have both thresholds") {
      val strategy = CommitStrategy.BySizeOrTime(50, 2.seconds)
      strategy.size shouldBe 50
      strategy.interval shouldBe 2.seconds
    }

    // ─────────────────────────────────────────────────────────────
    // flatMapMerge Concurrency - Demonstrating the Problem
    // ─────────────────────────────────────────────────────────────

    test("flatMapMerge with concurrency causes out-of-order processing completion") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val consumedOrder = CopyOnWriteArrayList<Int>()
      val processingCompletedOrder = CopyOnWriteArrayList<Int>()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer
          .consume(topicConfig)
          .take(5)
          .flatMapMerge(concurrency = 5) { ackRecord ->
            flow {
              val index = ackRecord.record.value().toInt()
              consumedOrder.add(index)

              // Earlier records take LONGER - causes out-of-order completion
              val processingTime = (5 - index) * 100L
              delay(processingTime.milliseconds)

              processingCompletedOrder.add(index)
              ackRecord.acknowledge()

              emit(index)
            }
          }.toList()
      }

      delay(500.milliseconds)

      repeat(5) { i ->
        kafkaTemplate.send(topic, "key-$i", i.toString()).get()
      }

      job.await()
      consumer.stop()

      // Consumption order is in order
      consumedOrder shouldContainExactly listOf(0, 1, 2, 3, 4)

      // Processing completion is REVERSED (out of order!)
      processingCompletedOrder shouldContainExactly listOf(4, 3, 2, 1, 0)
    }

    test("flatMapMerge demonstrates offset gap risk") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      data class AckEvent(
        val offset: Long,
        val ackTimeMs: Long
      )
      val ackEvents = CopyOnWriteArrayList<AckEvent>()
      val startTime = System.currentTimeMillis()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer
          .consume(topicConfig)
          .take(3)
          .flatMapMerge(concurrency = 3) { ackRecord ->
            flow {
              val offset = ackRecord.record.offset()

              // Offset 0 takes longest, offset 2 is fastest
              val processingTime = when (offset) {
                0L -> 300.milliseconds
                1L -> 200.milliseconds
                else -> 50.milliseconds
              }
              delay(processingTime)

              ackRecord.acknowledge()
              ackEvents.add(AckEvent(offset, System.currentTimeMillis() - startTime))

              emit(offset)
            }
          }.toList()
      }

      delay(500.milliseconds)

      repeat(3) { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      job.await()
      consumer.stop()

      ackEvents shouldHaveSize 3

      // Out-of-order acknowledgment: offset 2 was ack'd before offset 0
      val offsetOrder = ackEvents.sortedBy { it.ackTimeMs }.map { it.offset }
      offsetOrder.first() shouldBe 2L // Offset 2 acknowledged first
      offsetOrder.last() shouldBe 0L // Offset 0 acknowledged last

      // This is the "offset gap" problem - if crash after ack'ing 2 but before 0,
      // on restart offset 0 would be skipped!
    }

    // ─────────────────────────────────────────────────────────────
    // OrderedCommitter Unit Tests
    // ─────────────────────────────────────────────────────────────

    test("OrderedCommitter with BySize(1) commits immediately when contiguous") {
      // BySize(1) is per-record commit - commits immediately when contiguous offsets found
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val ackCalls = CopyOnWriteArrayList<Long>()

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
      // After 2: completed={2}, lastCommitted=-1 → can't commit (gap at 0,1)
      // After 0: completed={0,2}, lastCommitted=-1 → commit 0, lastCommitted=0
      // After 1: completed={1,2}, lastCommitted=0 → commit up to 2 (only ack 2), lastCommitted=2
      // After 4: completed={4}, lastCommitted=2 → can't commit (gap at 3)
      // After 3: completed={3,4}, lastCommitted=2 → commit up to 4 (only ack 4), lastCommitted=4

      results[0] shouldBe null // Offset 2: waiting for 0,1
      results[1]?.get(0) shouldBe 0L // Offset 0: commit 0
      results[2]?.get(0) shouldBe 2L // Offset 1: commit up to 2
      results[3] shouldBe null // Offset 4: waiting for 3
      results[4]?.get(0) shouldBe 4L // Offset 3: commit up to 4

      // Only highest contiguous offsets were ack'd (optimization: single commit per batch)
      // In Kafka, committing offset N means "processed up to N" so we don't need to commit intermediates
      ackCalls shouldContainExactly listOf(0L, 2L, 4L)
    }

    test("OrderedCommitter with BySize(n) batches commits") {
      // BySize(3) commits when 3 records have completed
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(3))
      val ackCalls = CopyOnWriteArrayList<Long>()

      // Add 2 records - should not commit yet
      committer.onComplete(CompletionEvent(0, 0L) { ackCalls.add(0L) }) shouldBe null
      committer.onComplete(CompletionEvent(0, 1L) { ackCalls.add(1L) }) shouldBe null

      ackCalls shouldHaveSize 0 // No commits yet

      // 3rd record triggers commit
      val result = committer.onComplete(CompletionEvent(0, 2L) { ackCalls.add(2L) })
      result?.get(0) shouldBe 2L

      // Only highest offset ack'd
      ackCalls shouldContainExactly listOf(2L)
    }

    test("OrderedCommitter handles multiple partitions independently") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))

      // Partition 0: offsets 1, 0 (out of order)
      committer.onComplete(CompletionEvent(0, 1L) {}) shouldBe null
      committer.onComplete(CompletionEvent(0, 0L) {})?.get(0) shouldBe 1L

      // Partition 1: offsets 0, 2, 1 (out of order)
      committer.onComplete(CompletionEvent(1, 0L) {})?.get(1) shouldBe 0L
      committer.onComplete(CompletionEvent(1, 2L) {}) shouldBe null
      committer.onComplete(CompletionEvent(1, 1L) {})?.get(1) shouldBe 2L

      val stats = committer.getStats()
      stats[0]?.lastCommitted shouldBe 1L // Partition 0 last committed: 1
      stats[1]?.lastCommitted shouldBe 2L // Partition 1 last committed: 2
    }

    test("OrderedCommitter tracks pending offsets in stats") {
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(100)) // Large batch to prevent auto-commit

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

      committer.onComplete(CompletionEvent(0, highOffset) { ackCalls.add(highOffset) })

      // Can't commit - waiting for previous offsets
      ackCalls shouldHaveSize 0

      committer.flush()
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

    // ─────────────────────────────────────────────────────────────
    // OrderedCommitter Integration Tests with Kafka
    // ─────────────────────────────────────────────────────────────

    test("OrderedCommitter with flatMapMerge prevents offset gaps") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      // BySize(1) for per-record commits in this test
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val commitChannel = createCommitChannel()

      val processingCompletionOrder = CopyOnWriteArrayList<Long>()
      val actualCommits = CopyOnWriteArrayList<Long>()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        coroutineScope {
          // Launch committer in background
          val committerJob = launchCommitter(committer, commitChannel)

          // Processing flow with high concurrency
          consumer
            .consume(topicConfig)
            .take(5)
            .flatMapMerge(concurrency = 5) { ackRecord ->
              flow {
                val offset = ackRecord.record.offset()
                val partition = ackRecord.record.partition()

                // Later offsets finish first
                delay(((4 - offset) * 100).milliseconds)

                processingCompletionOrder.add(offset)

                // Send to committer instead of direct ack
                commitChannel.send(
                  CompletionEvent(
                    partition = partition,
                    offset = offset,
                    acknowledge = {
                      actualCommits.add(offset)
                      ackRecord.acknowledge()
                    }
                  )
                )

                emit(offset)
              }
            }.toList()

          commitChannel.close()
          committerJob.join()
        }
      }

      delay(500.milliseconds)

      repeat(5) { i ->
        kafkaTemplate.send(topic, "key-$i", i.toString()).get()
      }

      job.await()
      consumer.stop()

      // Processing completed out of order
      processingCompletionOrder shouldContainExactly listOf(4L, 3L, 2L, 1L, 0L)

      // Optimized: only the highest contiguous offset gets committed
      // Since processing order is 4,3,2,1,0:
      // - Offsets 4,3,2,1 arrive: can't commit (waiting for 0)
      // - Offset 0 arrives: now 0-4 are contiguous → commit only offset 4
      // This is a single commit that covers all 5 records (Kafka semantics)
      actualCommits shouldContainExactly listOf(4L)
    }

    test("OrderedCommitter handles multiple partitions with concurrent processing") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 3)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val commits = CopyOnWriteArrayList<Pair<Int, Long>>()
      val committer = OrderedCommitter(
        commitStrategy = CommitStrategy.BySize(1),
        onCommit = { partition, offset -> commits.add(partition to offset) }
      )
      val commitChannel = createCommitChannel()

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        coroutineScope {
          val committerJob = launchCommitter(committer, commitChannel)

          consumer
            .consume(topicConfig)
            .take(9)
            .flatMapMerge(concurrency = 9) { ackRecord ->
              flow {
                val offset = ackRecord.record.offset()
                val partition = ackRecord.record.partition()

                delay((10..100).random().milliseconds)

                commitChannel.send(
                  CompletionEvent(
                    partition = partition,
                    offset = offset,
                    acknowledge = { ackRecord.acknowledge() }
                  )
                )

                emit(Unit)
              }
            }.toList()

          commitChannel.close()
          committerJob.join()
        }
      }

      delay(500.milliseconds)

      repeat(9) { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      job.await()
      consumer.stop()

      // Should have commits for multiple partitions
      commits.isNotEmpty() shouldBe true

      // Each partition's commits should be in order
      commits.groupBy { it.first }.forEach { (_, partitionCommits) ->
        val offsets = partitionCommits.map { it.second }
        offsets shouldBe offsets.sorted()
      }
    }

    test("OrderedCommitter with high concurrency processes all records") {
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      // BySize(1) to ensure all records get committed
      val committer = OrderedCommitter(commitStrategy = CommitStrategy.BySize(1))
      val commitChannel = createCommitChannel()
      val ackCount = AtomicInteger(0)

      val config = TestHelpers.testListenerConfig()
      val consumer = FlowKafkaConsumer(consumerFactory, config)
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        coroutineScope {
          val committerJob = launchCommitter(committer, commitChannel)

          consumer
            .consume(topicConfig)
            .take(20)
            .flatMapMerge(concurrency = 10) { ackRecord ->
              flow {
                delay(50.milliseconds)

                commitChannel.send(
                  CompletionEvent(
                    partition = ackRecord.record.partition(),
                    offset = ackRecord.record.offset(),
                    acknowledge = {
                      ackCount.incrementAndGet()
                      ackRecord.acknowledge()
                    }
                  )
                )

                emit(Unit)
              }
            }.toList()

          commitChannel.close()
          committerJob.join()
        }
      }

      delay(500.milliseconds)

      repeat(20) { i ->
        kafkaTemplate.send(topic, "key-$i", "value-$i").get()
      }

      job.await()
      consumer.stop()

      // All 20 records should be acknowledged
      ackCount.get() shouldBe 20

      // Committer stats should show last committed as 19
      val stats = committer.getStats()[0]!!
      stats.lastCommitted shouldBe 19L
      stats.pendingCount shouldBe 0
    }

    // ─────────────────────────────────────────────────────────────
    // FlowKafkaConsumer Integration with orderedCommits=true
    // ─────────────────────────────────────────────────────────────

    test("FlowKafkaConsumer ensures safe commit order with concurrent processing") {
      /**
       * FlowKafkaConsumer uses OrderedCommitter internally by default.
       * Even though processing completes out of order (4,3,2,1,0),
       * the OrderedCommitter ensures acks happen in offset order (0,1,2,3,4).
       *
       * Note: We can't directly observe the commit order from user code, but we verify
       * that all records are processed and acknowledged without errors.
       */
      val topic = TestHelpers.uniqueTopicName()
      kafka.createTopic(topic, partitions = 1)

      val groupId = TestHelpers.uniqueGroupId()
      val kafkaTemplate = kafka.createStringKafkaTemplate()
      val consumerFactory = kafka.createStringConsumerFactory(groupId)

      val processingOrder = CopyOnWriteArrayList<Long>()
      val processedCount = AtomicInteger(0)

      val config = ListenerConfig(
        concurrency = 1,
        pollTimeout = 500.milliseconds,
        backpressure = BackpressureConfig(enabled = false)
      )

      // FlowKafkaConsumer always uses OrderedCommitter internally
      val consumer = FlowKafkaConsumer(
        consumerFactory = consumerFactory,
        listenerConfig = config
      )
      val topicConfig = TopicConfig(name = topic)

      val job = async {
        consumer
          .consume(topicConfig)
          .take(5)
          .flatMapMerge(concurrency = 5) { ackRecord ->
            flow {
              val offset = ackRecord.record.offset()

              // Later offsets complete first (out of order processing)
              delay(((4 - offset) * 100).milliseconds)

              processingOrder.add(offset)

              // This acknowledge() goes through OrderedCommitter!
              // The committer holds off actual commits until offsets are contiguous
              ackRecord.acknowledge()
              processedCount.incrementAndGet()

              emit(offset)
            }
          }.toList()
      }

      delay(500.milliseconds)

      repeat(5) { i ->
        kafkaTemplate.send(topic, "key-$i", i.toString()).get()
      }

      job.await()
      consumer.stop()

      // Processing completed out of order (4, 3, 2, 1, 0)
      processingOrder shouldContainExactly listOf(4L, 3L, 2L, 1L, 0L)

      // All 5 records were processed and acknowledged (safely in order by OrderedCommitter)
      processedCount.get() shouldBe 5
    }
  })
