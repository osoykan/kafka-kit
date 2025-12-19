package io.github.osoykan.kafkaflow

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.poller.CommitStrategy
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.whileSelect
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.TreeSet
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * Represents a completed processing event that needs to be committed.
 *
 * @property partition The Kafka partition this record belongs to
 * @property offset The offset of the completed record
 * @property acknowledge Callback to actually commit the offset (calls Spring Kafka's ack)
 */
data class CompletionEvent(
  val partition: Int,
  val offset: Long,
  val acknowledge: () -> Unit
)

/**
 * Result of a commit operation.
 *
 * @property commits Map of partition to highest committed offset (empty if nothing committed)
 */
@JvmInline
value class CommitResult(
  val commits: Map<Int, Long>
) {
  val isEmpty: Boolean get() = commits.isEmpty()
  val isNotEmpty: Boolean get() = commits.isNotEmpty()

  inline fun forEach(action: (partition: Int, offset: Long) -> Unit) {
    commits.forEach { (partition, offset) -> action(partition, offset) }
  }

  companion object {
    val Empty = CommitResult(emptyMap())
  }
}

/**
 * Tracks uncommitted offsets for a batch commit.
 *
 * Uses [Mutex] for coroutine-friendly synchronization instead of @Synchronized.
 * This suspends instead of blocking threads.
 *
 * For each partition, we track:
 * - Completed offsets (may arrive out of order)
 * - Last committed offset
 * - Pending acknowledgment callbacks
 *
 * Only commits the highest contiguous offset per partition.
 */
internal class CommitBatch {
  private data class PartitionState(
    val completed: TreeSet<Long> = TreeSet(),
    var lastCommitted: Long = -1L,
    val pendingAcks: MutableMap<Long, () -> Unit> = mutableMapOf()
  )

  private val partitions = mutableMapOf<Int, PartitionState>()
  private val count = AtomicInteger(0)
  private val mutex = Mutex()

  /**
   * Records a completion event.
   * @return current count of uncommitted records
   */
  suspend fun addCompletion(event: CompletionEvent): Int = mutex.withLock {
    val state = partitions.getOrPut(event.partition) { PartitionState() }
    state.completed.add(event.offset)
    state.pendingAcks[event.offset] = event.acknowledge
    count.incrementAndGet()
  }

  /**
   * Gets the count of uncommitted records.
   */
  fun uncommittedCount(): Int = count.get()

  /**
   * Commits all contiguous offsets for all partitions.
   * Only acknowledges the highest contiguous offset per partition (Kafka semantics).
   *
   * @return CommitResult with map of partition to highest committed offset
   */
  suspend fun commitContiguous(): CommitResult = mutex.withLock {
    val committed = mutableMapOf<Int, Long>()

    partitions.forEach { (partition, state) ->
      val highestContiguous = findHighestContiguous(state)

      if (highestContiguous >= 0 && highestContiguous > state.lastCommitted) {
        // Get ack for highest offset before cleanup
        val highestAck = state.pendingAcks[highestContiguous]

        // Clean up all contiguous offsets
        for (offset in (state.lastCommitted + 1)..highestContiguous) {
          state.pendingAcks.remove(offset)
          state.completed.remove(offset)
          count.decrementAndGet()
        }

        // Only acknowledge the highest (Kafka semantics)
        highestAck?.invoke()

        state.lastCommitted = highestContiguous
        committed[partition] = highestContiguous

        logger.debug { "Partition $partition: committed up to offset $highestContiguous" }
      }
    }

    CommitResult(committed)
  }

  /**
   * Finds the highest contiguous offset from lastCommitted.
   * @return highest contiguous offset, or -1 if none found
   */
  private fun findHighestContiguous(state: PartitionState): Long {
    var nextExpected = state.lastCommitted + 1
    var highest = -1L

    while (state.completed.contains(nextExpected)) {
      highest = nextExpected
      nextExpected++
    }

    return highest
  }

  /**
   * Force commits the highest pending offset per partition (even with gaps).
   * Use during shutdown to prevent stuck records.
   */
  suspend fun flush(): CommitResult = mutex.withLock {
    val flushed = mutableMapOf<Int, Long>()

    partitions.forEach { (partition, state) ->
      if (state.pendingAcks.isNotEmpty()) {
        val maxOffset = state.pendingAcks.keys.max()
        state.pendingAcks[maxOffset]?.invoke()
        state.pendingAcks.clear()
        state.completed.clear()
        state.lastCommitted = maxOffset
        flushed[partition] = maxOffset
        logger.warn { "Partition $partition: flushed up to $maxOffset (may have gaps!)" }
      }
    }

    count.set(0)
    CommitResult(flushed)
  }

  /**
   * Gets stats for all partitions.
   */
  suspend fun getStats(): Map<Int, CommitterStats> = mutex.withLock {
    partitions.mapValues { (_, state) ->
      CommitterStats(
        lastCommitted = state.lastCommitted,
        pendingCount = state.completed.size,
        pendingOffsets = state.completed.toList()
      )
    }
  }

  /**
   * Resets state for a specific partition.
   */
  suspend fun resetPartition(partition: Int): Unit = mutex.withLock {
    partitions.remove(partition)?.let { state ->
      count.addAndGet(-state.pendingAcks.size)
    }
  }

  /**
   * Resets all state.
   */
  suspend fun reset(): Unit = mutex.withLock {
    partitions.clear()
    count.set(0)
  }
}

/**
 * Ordered Committer that ensures Kafka offsets are committed without gaps.
 *
 * ## Problem
 * With concurrent processing (`flatMapMerge(concurrency > 1)`), records may complete
 * out of order. If we commit immediately on completion, we risk offset gaps.
 *
 * ## Solution
 * Track completed offsets per partition and only commit when offsets are contiguous.
 * Uses [CommitStrategy] to batch commits for efficiency.
 *
 * ## Commit Strategies
 *
 * - **BySize(1)**: Commit immediately when contiguous offsets are found (per-record, safest)
 * - **BySize(n)**: Commit when n records have completed (batched, higher throughput)
 * - **ByTime(interval)**: Commit every interval (predictable latency)
 * - **BySizeOrTime(n, interval)**: Commit on whichever comes first (balanced)
 *
 * For `BySizeOrTime`, uses Kotlin's `whileSelect` to handle concurrent timer and count
 * events properly - only ONE event is processed at a time, preventing race conditions.
 *
 * @param commitStrategy Strategy for batching commits (default: BySize(100) for throughput)
 * @param onCommit Optional callback invoked after each commit with partition and offset
 */
class OrderedCommitter(
  private val commitStrategy: CommitStrategy = CommitStrategy.BySize(100),
  private val onCommit: (partition: Int, offset: Long) -> Unit = { _, _ -> }
) {
  private val batch = CommitBatch()
  private val batchSignal = Channel<Unit>(Channel.RENDEZVOUS)
  private var commitManagerJob: Job? = null
  private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

  /**
   * Called when a record processing completes.
   * For BySize(1), commits immediately if contiguous.
   * For other strategies, signals the commit manager.
   *
   * @param event The completion event with partition, offset, and ack callback
   * @return The committed offsets (empty if no commit was made)
   */
  suspend fun onComplete(event: CompletionEvent): CommitResult {
    val count = batch.addCompletion(event)

    return when (commitStrategy) {
      is CommitStrategy.BySize -> {
        if (count >= commitStrategy.size) {
          doCommit()
        } else {
          CommitResult.Empty
        }
      }

      is CommitStrategy.ByTime -> {
        // Timer handles commits via commit manager
        CommitResult.Empty
      }

      is CommitStrategy.BySizeOrTime -> {
        if (count >= commitStrategy.size) {
          // Signal commit manager that batch size reached
          batchSignal.trySend(Unit)
        }
        // Actual commit happens in commit manager
        CommitResult.Empty
      }
    }
  }

  /**
   * Performs the commit operation.
   */
  private suspend fun doCommit(): CommitResult {
    val result = batch.commitContiguous()
    result.forEach { partition, offset -> onCommit(partition, offset) }
    return result
  }

  /**
   * Starts the commit manager.
   * Uses whileSelect to handle ByTime and BySizeOrTime strategies properly.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  fun start() {
    if (commitManagerJob != null) return

    commitManagerJob = scope.launch {
      when (commitStrategy) {
        is CommitStrategy.BySize -> {
          // No background manager needed for BySize - commits happen inline
        }

        is CommitStrategy.ByTime -> {
          // Commit on time interval only
          while (isActive) {
            delay(commitStrategy.interval)
            if (batch.uncommittedCount() > 0) {
              doCommit()
            }
          }
        }

        is CommitStrategy.BySizeOrTime -> {
          // Use whileSelect to handle both size and time triggers
          // Only ONE event is processed at a time, preventing race conditions
          whileSelect {
            batchSignal.onReceiveCatching { result ->
              if (!result.isClosed) {
                doCommit()
              }
              !result.isClosed
            }
            onTimeout(commitStrategy.interval) {
              if (batch.uncommittedCount() > 0) {
                doCommit()
              }
              true
            }
          }
        }
      }
    }

    logger.debug { "Started commit manager with strategy: $commitStrategy" }
  }

  /**
   * Stops the commit manager.
   */
  fun stop() {
    commitManagerJob?.cancel()
    commitManagerJob = null
    batchSignal.close()
  }

  /**
   * Processes completion events from a channel until the channel is closed.
   */
  suspend fun processChannel(channel: ReceiveChannel<CompletionEvent>) {
    start()
    try {
      for (event in channel) {
        onComplete(event)
      }
    } finally {
      stop()
      logger.debug { "Committer channel closed, flushing remaining..." }
      flush()
    }
  }

  /**
   * Forces commit of all pending completions (highest contiguous per partition).
   * Use this during shutdown.
   */
  suspend fun flush() {
    val result = batch.flush()
    result.forEach { partition, offset -> onCommit(partition, offset) }
    stop()
  }

  /**
   * Returns statistics about the committer state.
   */
  suspend fun getStats(): Map<Int, CommitterStats> = batch.getStats()

  /**
   * Resets state for a partition. Use when partition is revoked.
   */
  suspend fun resetPartition(partition: Int) {
    batch.resetPartition(partition)
    logger.debug { "Reset state for partition $partition" }
  }

  /**
   * Resets all state. Use during shutdown or testing.
   */
  suspend fun reset() {
    batch.reset()
    stop()
    logger.debug { "Reset all committer state" }
  }
}

/**
 * Statistics for a single partition in the committer.
 */
data class CommitterStats(
  val lastCommitted: Long,
  val pendingCount: Int,
  val pendingOffsets: List<Long>
)

/**
 * Creates a channel for sending completion events to an [OrderedCommitter].
 */
fun createCommitChannel(
  capacity: Int = Channel.UNLIMITED
): Channel<CompletionEvent> = Channel(capacity)

/**
 * Launches the committer as a background coroutine.
 */
fun CoroutineScope.launchCommitter(
  committer: OrderedCommitter,
  channel: ReceiveChannel<CompletionEvent>
) = launch(Dispatchers.Default) {
  committer.processChannel(channel)
}
