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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class CommitOrderIntegrationTests :
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
            .onEach { ackRecord ->
              // Register offset BEFORE concurrent processing (in delivery order)
              committer.registerOffset(ackRecord.record.partition(), ackRecord.record.offset())
            }.flatMapMerge(concurrency = 5) { ackRecord ->
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
      // - Offset 0 arrives: now 0-4 are contiguous -> commit only offset 4
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
            .onEach { ackRecord ->
              committer.registerOffset(ackRecord.record.partition(), ackRecord.record.offset())
            }.flatMapMerge(concurrency = 9) { ackRecord ->
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
            .onEach { ackRecord ->
              committer.registerOffset(ackRecord.record.partition(), ackRecord.record.offset())
            }.flatMapMerge(concurrency = 10) { ackRecord ->
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
