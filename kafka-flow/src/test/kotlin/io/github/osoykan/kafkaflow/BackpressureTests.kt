package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.measureTime

/**
 * Tests to verify backpressure behavior in Kafka Flow.
 *
 * Backpressure is the mechanism that prevents a fast producer from overwhelming
 * a slow consumer. When the buffer is full, the producer blocks until the
 * consumer catches up.
 */
class BackpressureTests :
  FunSpec({

    test("buffer capacity should limit how many items are buffered") {
      val bufferSize = 5
      val produced = AtomicInteger(0)
      val consumed = AtomicInteger(0)

      // Create a flow that simulates a fast producer
      val flow = flow {
        repeat(20) { i ->
          produced.incrementAndGet()
          emit(i)
        }
      }.buffer(bufferSize)

      // Slow consumer - takes 50ms per item
      val job = CoroutineScope(Dispatchers.Default).launch {
        flow.collect { _ ->
          consumed.incrementAndGet()
          delay(50.milliseconds)
        }
      }

      // Wait for initial buffer fill
      delay(100.milliseconds)

      // Producer should have filled buffer + 1 (the one being processed)
      // but not all 20 items
      val producedCount = produced.get()
      producedCount shouldBeLessThan 20

      job.join()

      // All items should eventually be consumed
      consumed.get() shouldBe 20
    }

    test("Channel.RENDEZVOUS provides immediate backpressure") {
      val produced = AtomicInteger(0)
      val consumed = AtomicInteger(0)

      // RENDEZVOUS = 0 capacity, producer blocks until consumer receives
      val flow = flow {
        repeat(5) { i ->
          produced.incrementAndGet()
          emit(i)
        }
      }.buffer(Channel.RENDEZVOUS)

      val job = CoroutineScope(Dispatchers.Default).launch {
        flow.collect { _ ->
          consumed.incrementAndGet()
          delay(100.milliseconds)
        }
      }

      // With RENDEZVOUS, production is tightly coupled to consumption
      // After 250ms, we should have consumed ~2-3 items
      delay(250.milliseconds)
      val consumedSoFar = consumed.get()
      consumedSoFar shouldBeLessThan 5

      job.join()
      consumed.get() shouldBe 5
    }

    test("larger buffer allows more items to be produced ahead") {
      val smallBuffer = 2
      val largeBuffer = 50

      // Measure how many items can be produced immediately with small buffer
      val smallBufferProduced = measureImmediateProduction(smallBuffer, totalItems = 100)

      // Measure how many items can be produced immediately with large buffer
      val largeBufferProduced = measureImmediateProduction(largeBuffer, totalItems = 100)

      // Large buffer should allow more immediate production
      largeBufferProduced shouldBeGreaterThan smallBufferProduced
    }

    test("backpressure prevents memory overflow with slow consumer") {
      val bufferSize = 10
      val itemCount = 1000
      val maxBuffered = AtomicInteger(0)
      val currentlyBuffered = AtomicInteger(0)

      val flow = flow {
        repeat(itemCount) { i ->
          val buffered = currentlyBuffered.incrementAndGet()
          maxBuffered.updateAndGet { max -> maxOf(max, buffered) }
          emit(i)
        }
      }.buffer(bufferSize)

      runBlocking {
        flow.collect { _ ->
          currentlyBuffered.decrementAndGet()
          // Simulate slow processing
          delay(1.milliseconds)
        }
      }

      // Max buffered should be bounded by buffer size + some overhead
      // (not all 1000 items at once)
      maxBuffered.get() shouldBeLessThan 100
    }

    test("callbackFlow with trySendBlocking provides backpressure") {
      val bufferSize = 5
      val consumed = AtomicInteger(0)
      val blocked = AtomicInteger(0)

      // Simulate Kafka listener behavior with callbackFlow
      val flow = callbackFlow<Int> {
        val producerJob = launch(Dispatchers.Default) {
          repeat(20) { i ->
            val beforeSend = System.currentTimeMillis()
            // This is what Kafka listener does - blocks when buffer is full
            trySendBlocking(i).getOrThrow()
            val afterSend = System.currentTimeMillis()
            if (afterSend - beforeSend > 10) {
              blocked.incrementAndGet()
            }
          }
          close()
        }
        awaitClose { producerJob.cancel() }
      }.buffer(bufferSize)

      runBlocking {
        flow.collect { _ ->
          consumed.incrementAndGet()
          delay(50.milliseconds)
        }
      }

      consumed.get() shouldBe 20
      // Should have blocked at least once due to backpressure
      blocked.get() shouldBeGreaterThan 0
    }

    test("flow processing time is bounded by consumer speed with backpressure") {
      val bufferSize = 10
      val itemCount = 20
      val processingTimePerItem = 25.milliseconds

      val duration = measureTime {
        val flow = flow {
          repeat(itemCount) { emit(it) }
        }.buffer(bufferSize)

        runBlocking {
          flow.collect { _ ->
            delay(processingTimePerItem)
          }
        }
      }

      // Total time should be at least itemCount * processingTime
      // (backpressure ensures we can't skip processing)
      val expectedMinTime = itemCount * processingTimePerItem.inWholeMilliseconds
      duration.inWholeMilliseconds shouldBeGreaterThan (expectedMinTime - 100)
    }

    test("cancellation during processing should interrupt the flow immediately") {
      val processed = AtomicInteger(0)
      val flow = flow {
        repeat(10) { i ->
          emit(i)
        }
      }

      val scope = CoroutineScope(Dispatchers.Default)
      val job = scope.launch {
        flow.collect {
          delay(1000.milliseconds) // Long processing
          processed.incrementAndGet()
        }
      }

      // Wait a bit and then cancel
      delay(150.milliseconds)
      job.cancelAndJoin()

      // Should have processed 0 items because the first one was cancelled during its long delay
      processed.get() shouldBe 0
    }
  })

/**
 * Measures how many items can be produced immediately before blocking.
 */
private suspend fun measureImmediateProduction(bufferSize: Int, totalItems: Int): Int {
  val produced = AtomicInteger(0)

  val flow = flow {
    repeat(totalItems) { i ->
      emit(i)
      produced.incrementAndGet()
    }
  }.buffer(bufferSize)

  val job = CoroutineScope(Dispatchers.Default).launch {
    flow.collect { _ ->
      // Very slow consumer
      delay(100.milliseconds)
    }
  }

  // Wait a short time for initial production burst
  delay(50.milliseconds)

  val immediatelyProduced = produced.get()
  job.cancel()

  return immediatelyProduced
}
