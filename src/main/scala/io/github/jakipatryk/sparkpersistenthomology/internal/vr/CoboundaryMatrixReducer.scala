package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, udf }
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.LocalPivotChunksStatistics
import scala.collection.mutable.LongMap

import org.apache.spark.TaskContext

object CoboundaryMatrixReducer {

  /** Reduces coboundary matrix for a single dim. Reduced matrix is guaranteed to have unique
    * non-empty pivots, where column of simplex `i` having pivot `j` means that there is persistence
    * pair (`i`, `j`), and column `i` having empty pivot means that there is an essential
    * persistence pair (`i`, inf).
    */
  def reduce(
    coboundaryMatrix: Dataset[CoboundaryMatrixColumn],
    pivotStatsAccumulator: PivotChunksStatisticsAccumulator
  )(implicit context: FiltrationContext, spark: SparkSession): Dataset[CoboundaryMatrixColumn] = {
    import spark.implicits._

    // var currentMatrix   = coboundaryMatrix
    // var hasPivotChanged = true
    //
    // while (hasPivotChanged) {
    //   val partitionedAndSortedMatrix = repartitionAndSort(
    //     currentMatrix,
    //     pivotStatsAccumulator
    //   )
    //
    //   pivotStatsAccumulator.reset()
    //
    //   val nextMatrix = partitionedAndSortedMatrix.mapPartitions { partition =>
    //     val (reducedIterator, localStats) = reducePartition(partition, pivotStatsAccumulator)
    //     TaskContext
    //       .get()
    //       .addTaskCompletionListener[Unit](_ => pivotStatsAccumulator.add(localStats))
    //     reducedIterator
    //   }
    //
    //   currentMatrix = nextMatrix.localCheckpoint()
    //
    //   hasPivotChanged = pivotStatsAccumulator.value.hasPivotChanged
    // }
    //
    // currentMatrix
    //
    coboundaryMatrix
      .coalesce(1)
      .sortWithinPartitions(CoboundaryMatrixColumn.matrixColumnsOrderingExpressions: _*)
      .mapPartitions(p => reducePartition(p, pivotStatsAccumulator)._1)
      .localCheckpoint()
  }

  private def repartitionAndSort(
    coboundaryMatrix: Dataset[CoboundaryMatrixColumn],
    pivotStatsAccumulator: PivotChunksStatisticsAccumulator
  )(implicit spark: SparkSession): Dataset[CoboundaryMatrixColumn] = {
    import spark.implicits._

    val stats         = pivotStatsAccumulator.value
    val numPartitions = spark.conf.get("spark.sql.shuffle.partitions").toInt

    val partitionIdExpr = PartitioningUtils.getPartitionId(
      CoboundaryMatrixColumn.pivotExpression,
      stats,
      numPartitions
    )

    coboundaryMatrix
      .withColumn("_partition_id", partitionIdExpr)
      .repartition(numPartitions, col("_partition_id"))
      .sortWithinPartitions(CoboundaryMatrixColumn.matrixColumnsOrderingExpressions: _*)
      .drop("_partition_id")
      .as[CoboundaryMatrixColumn]
  }

  private def reducePartition(
    partition: Iterator[CoboundaryMatrixColumn],
    pivotStatsAccumulator: PivotChunksStatisticsAccumulator
  )(implicit
    context: FiltrationContext
  ): (Iterator[CoboundaryMatrixColumn], LocalPivotChunksStatistics) = {
    val stats    = pivotStatsAccumulator.createLocalStats()
    val pivotMap = LongMap.empty[CoboundaryMatrixColumn]

    val reducedIterator = partition.map { col =>
      var currentCol = col

      var pOpt   = currentCol.pivot
      var isDone = false
      while (pOpt.isDefined && !isDone) {
        val p      = pOpt.get
        val pIndex = p.index

        pivotMap.get(pIndex) match {
          case Some(prevCol) =>
            stats.hasPivotChanged = true
            currentCol = currentCol + prevCol
            pOpt = currentCol.valueTopEntries.headOption
          case None =>
            ApparentPairsDetector.getBirthIfIsDeathOfApparentPair(p) match {
              case Some(birthSimplex) =>
                stats.hasPivotChanged = true
                currentCol = currentCol + CoboundaryMatrixColumn(birthSimplex)
                pOpt = currentCol.pivot
              case None =>
                // New pivot found!
                pivotMap.put(pIndex, currentCol)
                stats.addPivot(pIndex)
                isDone = true
            }
        }
      }

      currentCol
    }

    (reducedIterator, stats)
  }

}
