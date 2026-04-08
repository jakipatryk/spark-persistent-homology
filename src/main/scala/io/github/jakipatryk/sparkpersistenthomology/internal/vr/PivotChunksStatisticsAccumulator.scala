package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.{
  LocalPivotChunksStatistics,
  PivotChunksStatistics
}
import org.apache.spark.util.AccumulatorV2

/** Accumulator for counting the number of pivots falling into uniformly sized chunks.
  *
  * This accumulator is optimized for very high-frequency local updates within Spark tasks. It uses
  * a lightweight [[LocalPivotChunksStatistics]] backed by an Array during execution, minimizing
  * allocation and overhead. Upon evaluation, it returns a [[PivotChunksStatistics]] providing
  * explicit ranges and counts for partitioner optimizations.
  *
  * @example
  *   {{{
  *   // Setup on driver
  *   val statsAcc = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(chunkSize = 10, numberOfSimplices = 100))
  *   sparkContext.register(statsAcc, "PivotStats")
  *
  *   // Use in mapPartitions on a Dataset
  *   val mappedDataset = dataset.mapPartitions { iter =>
  *     // Create thread-local state to avoid continuous accumulator synchronization
  *     val localStats = new LocalPivotChunksStatistics(chunkSize = 10, numberOfSimplices = 100)
  *
  *     // Register the accumulator update to run when the task completes
  *     org.apache.spark.TaskContext.get().addTaskCompletionListener[Unit](_ => statsAcc.add(localStats))
  *
  *     iter.map { simplex =>
  *       val pivot = calculatePivot(simplex)
  *       localStats.addPivot(pivot)
  *       simplex
  *     }
  *   }
  *
  *   mappedDataset.count() // Action triggers execution
  *
  *   // Read on driver
  *   val results: PivotChunksStatistics = statsAcc.value
  *   results.chunks.foreach { chunk =>
  *     println(s"Pivots between ${chunk.pivotsStart} and ${chunk.pivotsEnd}: ${chunk.count}")
  *   }
  *   }}}
  */
private[sparkpersistenthomology] class PivotChunksStatisticsAccumulator(
  private[sparkpersistenthomology] var state: LocalPivotChunksStatistics
) extends AccumulatorV2[LocalPivotChunksStatistics, PivotChunksStatistics] {

  override def isZero: Boolean = {
    if (state.hasPivotChanged) return false
    var i = 0
    while (i < state.counts.length) {
      if (state.counts(i) != 0L) return false
      i += 1
    }
    true
  }

  override def copy(): AccumulatorV2[LocalPivotChunksStatistics, PivotChunksStatistics] = {
    val copiedState =
      new LocalPivotChunksStatistics(state.chunkSize, state.numberOfSimplices, state.counts.clone())
    copiedState.hasPivotChanged = state.hasPivotChanged
    new PivotChunksStatisticsAccumulator(copiedState)
  }

  def createLocalStats(): LocalPivotChunksStatistics = {
    new LocalPivotChunksStatistics(state.chunkSize, state.numberOfSimplices)
  }

  override def reset(): Unit = {
    state = createLocalStats()
  }

  override def add(v: LocalPivotChunksStatistics): Unit = {
    state.hasPivotChanged ||= v.hasPivotChanged
    var i = 0
    while (i < state.counts.length && i < v.counts.length) {
      state.counts(i) += v.counts(i)
      i += 1
    }
  }

  override def merge(
    other: AccumulatorV2[LocalPivotChunksStatistics, PivotChunksStatistics]
  ): Unit = {
    other match {
      case o: PivotChunksStatisticsAccumulator =>
        state.hasPivotChanged ||= o.state.hasPivotChanged
        var i = 0
        while (i < state.counts.length && i < o.state.counts.length) {
          state.counts(i) += o.state.counts(i)
          i += 1
        }
      case _ =>
        throw new UnsupportedOperationException(s"Cannot merge with ${other.getClass.getName}")
    }
  }

  override def value: PivotChunksStatistics = {
    import PivotChunksStatisticsAccumulator.ChunkStatistics

    val chunks = Vector.tabulate(state.counts.length) { i =>
      val pivotsStart = i * state.chunkSize
      val pivotsEnd   = math.min((i + 1) * state.chunkSize - 1, state.numberOfSimplices - 1)
      ChunkStatistics(pivotsStart, pivotsEnd, state.counts(i))
    }
    PivotChunksStatistics(chunks, state.hasPivotChanged)
  }
}

private[sparkpersistenthomology] object PivotChunksStatisticsAccumulator {

  /** Lightweight, thread-local structure used inside executors to rapidly count pivots before
    * merging them into the global [[PivotChunksStatisticsAccumulator]].
    *
    * Backed by a raw `Array[Long]` for high performance.
    *
    * @example
    *   {{{
    *   val localStats = new LocalPivotChunksStatistics(chunkSize = 100, numberOfSimplices = 1000)
    *   localStats.addPivot(42)  // maps to chunk 0
    *   localStats.addPivot(150) // maps to chunk 1
    *   }}}
    */
  private[sparkpersistenthomology] class LocalPivotChunksStatistics(
    val chunkSize: Long,
    val numberOfSimplices: Long,
    val counts: Array[Long]
  ) extends Serializable {

    var hasPivotChanged: Boolean = false

    def this(chunkSize: Long, numberOfSimplices: Long) = {
      this(
        chunkSize,
        numberOfSimplices,
        new Array[Long](((numberOfSimplices + chunkSize - 1) / chunkSize).toInt)
      )
    }

    /** Increments the internal count for the chunk that handles the given pivot.
      * @param pivot
      *   The index of the pivot to account for.
      */
    def addPivot(pivot: Long): Unit = {
      counts((pivot / chunkSize).toInt) += 1L
    }
  }

  /** Final result structure mapping a contiguous range of pivots to their total count.
    */
  private[sparkpersistenthomology] case class ChunkStatistics(
    pivotsStart: Long,
    pivotsEnd: Long,
    count: Long
  )

  /** Final resulting state emitted by `.value` on the [[PivotChunksStatisticsAccumulator]].
    */
  private[sparkpersistenthomology] case class PivotChunksStatistics(
    chunks: Vector[ChunkStatistics],
    hasPivotChanged: Boolean
  )
}
