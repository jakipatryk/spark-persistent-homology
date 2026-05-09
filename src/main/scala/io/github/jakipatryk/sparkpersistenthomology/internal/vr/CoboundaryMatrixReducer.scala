package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.utils.SimplexIndex
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.collection.mutable.LongMap

import org.apache.spark.TaskContext
import org.apache.spark.util.LongAccumulator

object CoboundaryMatrixReducer {

  /** Reduces coboundary matrix for a single dim. Reduced matrix is guaranteed to have unique
    * non-empty pivots, where column of simplex `i` having pivot `j` means that there is persistence
    * pair (`i`, `j`), and column `i` having empty pivot means that there is an essential
    * persistence pair (`i`, inf).
    */
  def reduce(
    coboundaryMatrix: Dataset[CoboundaryMatrixColumn]
  )(implicit context: FiltrationContext, spark: SparkSession): Dataset[CoboundaryMatrixColumn] = {
    import spark.implicits._

    var currentMatrix          = coboundaryMatrix
    var shouldContinueReducing = true

    val explicitPartitions =
      spark.conf.get("spark.persistenthomology.vr.reducer.explicit.partitions", "10").toInt
    val apparentPartitions =
      spark.conf.get("spark.persistenthomology.vr.reducer.apparent.partitions", "200").toInt

    while (shouldContinueReducing) {
      val hasPivotChangedAcc = spark.sparkContext.longAccumulator

      // Phase 1: Explicit Matrix Exhaustive Reduction
      val partitionedAndSortedMatrix = currentMatrix
        .repartition(explicitPartitions, CoboundaryMatrixColumn.pivotExpression)
        .sortWithinPartitions(
          CoboundaryMatrixColumn.reverseColumnsFiltrationOrderingExpressions: _*
        )
        .as[CoboundaryMatrixColumn]

      val afterPhase1Matrix = partitionedAndSortedMatrix.mapPartitions { partition =>
        val (reducedIterator, hasPivotChanged) = reducePartitionExplicitMatrixExhaustive(partition)
        TaskContext
          .get()
          .addTaskCompletionListener[Unit] { _ =>
            if (hasPivotChanged()) hasPivotChangedAcc.add(1L)
          }
        reducedIterator
      }

      // Phase 2: Apparent Pair Shallow Matrix Reduction
      val nextMatrix = afterPhase1Matrix
        .repartition(apparentPartitions)
        .as[CoboundaryMatrixColumn]
        .mapPartitions { partition =>
          val (reducedIterator, hasPivotChanged) = reducePartitionApparentPairShallow(partition)
          TaskContext
            .get()
            .addTaskCompletionListener[Unit] { _ =>
              if (hasPivotChanged()) hasPivotChangedAcc.add(1L)
            }
          reducedIterator
        }

      val prevMatrix = currentMatrix
      currentMatrix = nextMatrix.localCheckpoint()
      prevMatrix.unpersist(false)

      shouldContinueReducing = !hasPivotChangedAcc.isZero
    }

    currentMatrix
  }

  private def reducePartitionExplicitMatrixExhaustive(
    partition: Iterator[CoboundaryMatrixColumn]
  )(implicit
    context: FiltrationContext
  ): (Iterator[CoboundaryMatrixColumn], () => Boolean) = {
    val pivotMap = scala.collection.mutable.HashMap.empty[SimplexIndex, CoboundaryMatrixColumn]
    var hasPivotChanged = false

    val reducedIterator = partition.map { col =>
      val mutableCol = MutableCoboundaryMatrixColumn(col)
      var pOpt       = mutableCol.pivot

      while (pOpt.isDefined && pivotMap.contains(pOpt.get.index)) {
        hasPivotChanged = true
        mutableCol += pivotMap(pOpt.get.index)
        pOpt = mutableCol.pivot
      }

      val definitivePivot = pOpt
      if (definitivePivot.isDefined) {
        mutableCol.dequeueToBuffer()
      }

      while (mutableCol.nonEmpty) {
        val currentElement = mutableCol.pivot.get
        if (pivotMap.contains(currentElement.index)) {
          mutableCol += pivotMap(currentElement.index)
        } else {
          mutableCol.dequeueToBuffer()
        }
      }

      val result = mutableCol.toImmutableAndDrain

      definitivePivot.map(_.index).foreach { p =>
        pivotMap.put(p, result)
      }

      result
    }

    (reducedIterator, () => hasPivotChanged)
  }

  private def reducePartitionApparentPairShallow(
    partition: Iterator[CoboundaryMatrixColumn]
  )(implicit context: FiltrationContext): (Iterator[CoboundaryMatrixColumn], () => Boolean) = {
    var hasPivotChanged = false

    val reducedIterator = partition.map { col =>
      val mutableCol           = MutableCoboundaryMatrixColumn(col)
      var pOpt                 = mutableCol.pivot
      var foundDefinitivePivot = false

      while (pOpt.isDefined && !foundDefinitivePivot) {
        val currentPivot = pOpt.get
        ApparentPairsDetector.getBirthIfIsDeathOfApparentPair(currentPivot) match {
          case Some(birthSimplex) =>
            hasPivotChanged = true
            mutableCol += birthSimplex
            pOpt = mutableCol.pivot
          case None =>
            foundDefinitivePivot = true
        }
      }

      mutableCol.toImmutableAndDrain
    }

    (reducedIterator, () => hasPivotChanged)
  }

}
