package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.PivotChunksStatistics

private[sparkpersistenthomology] object PartitioningUtils {

  /** Returns a Spark Column that maps a pivot to a partition ID based on the given statistics and
    * number of partitions.
    */
  def getPartitionId(
    pivot: Column,
    stats: PivotChunksStatistics,
    numPartitions: Int
  ): Column = {
    val boundaries = calculateOptimalBoundaries(stats, numPartitions)

    val getPartitionIdUdf = udf((p: Long) => {
      if (p == -1L) {
        0
      } else {
        val idx = java.util.Arrays.binarySearch(boundaries, p)
        if (idx < 0) -idx - 1 else idx
      }
    })

    getPartitionIdUdf(pivot)
  }

  private def calculateOptimalBoundaries(
    stats: PivotChunksStatistics,
    numPartitions: Int
  ): Array[Long] = {
    val totalPivots = stats.chunks.map(_.count).sum
    if (totalPivots == 0 || numPartitions <= 1) return Array.emptyLongArray

    val targetPerPartition = totalPivots / numPartitions
    val boundaries         = new Array[Long](numPartitions - 1)

    var currentCount  = 0L
    var boundaryIndex = 0

    for (i <- 0 until (stats.chunks.length - 1)) {
      val chunk = stats.chunks(i)
      currentCount += chunk.count

      val nextCount    = stats.chunks(i + 1).count
      val distCurrent  = math.abs(currentCount - targetPerPartition)
      val distWithNext = math.abs(currentCount + nextCount - targetPerPartition)

      if (distCurrent <= distWithNext && boundaryIndex < boundaries.length) {
        boundaries(boundaryIndex) = chunk.pivotsEnd
        boundaryIndex += 1
        currentCount = currentCount - targetPerPartition
      }
    }

    while (boundaryIndex < boundaries.length) {
      boundaries(boundaryIndex) =
        if (stats.chunks.nonEmpty) stats.chunks.last.pivotsEnd else Long.MaxValue
      boundaryIndex += 1
    }

    boundaries.distinct.sorted
  }
}
