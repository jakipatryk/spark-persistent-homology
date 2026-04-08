package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.TaskContext
import org.apache.spark.sql.{ Dataset, SparkSession }
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.LocalPivotChunksStatistics

private[sparkpersistenthomology] object CoboundaryMatrixConstructor {

  /** Constructs `Dataset` of [[CoboundaryMatrixColumn]] for columns that need to be reduced.
    *
    * It skips columns that are known to either become zero or be in zero-persistence pair after
    * reduction (clearing optimization and apparent pairs).
    *
    * Due to clearing optimization, all columns that are kept and are reduced to zero represent
    * birth of essential persistence pair (one without death).
    */
  def construct(
    dim: Byte,
    pivotStatsAccumulator: PivotChunksStatisticsAccumulator,
    previousDimResult: Option[Dataset[CoboundaryMatrixColumn]] = None
  )(implicit context: FiltrationContext, spark: SparkSession): Dataset[CoboundaryMatrixColumn] = {
    import spark.implicits._

    val numCombinations = context.cns.value.allCombinationsCount(Simplex.dimToCombinationSize(dim))
    val range           = spark.range(numCombinations).as[Long]

    val filteredRange = previousDimResult match {
      case Some(prev) =>
        range.except(prev.flatMap(_.pivot.toSeq))
      case None =>
        range
    }

    filteredRange.as[Long].mapPartitions { iter =>
      val localStats = pivotStatsAccumulator.createLocalStats()

      TaskContext.get().addTaskCompletionListener[Unit](_ => pivotStatsAccumulator.add(localStats))

      iter.flatMap { index =>
        val simplex = Simplex(index, dim)
        val shouldKeepTheSimplex = simplex.radius <= context.distanceThreshold &&
          !ApparentPairsDetector.isInZeroApparentPair(simplex)

        if (shouldKeepTheSimplex) {
          val column = CoboundaryMatrixColumn(simplex)
          column.pivot.foreach(localStats.addPivot)
          Some(column)
        } else {
          None
        }
      }
    }
  }

}
