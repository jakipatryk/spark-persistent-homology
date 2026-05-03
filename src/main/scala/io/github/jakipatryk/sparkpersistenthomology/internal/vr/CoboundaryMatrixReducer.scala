package io.github.jakipatryk.sparkpersistenthomology.internal.vr

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

    val numPartitions = spark.conf.get("spark.sql.shuffle.partitions").toInt

    while (shouldContinueReducing) {
      val hasPivotChangedAcc = spark.sparkContext.longAccumulator

      val partitionedAndSortedMatrix = currentMatrix
        .repartition(numPartitions, CoboundaryMatrixColumn.pivotExpression)
        .sortWithinPartitions(
          CoboundaryMatrixColumn.reverseColumnsFiltrationOrderingExpressions: _*
        )
        .as[CoboundaryMatrixColumn]

      val nextMatrix = partitionedAndSortedMatrix.mapPartitions { partition =>
        val (reducedIterator, hasPivotChanged) = reducePartition(partition)
        TaskContext
          .get()
          .addTaskCompletionListener[Unit] { _ =>
            if (hasPivotChanged()) hasPivotChangedAcc.add(1L)
          }
        reducedIterator
      }

      val prevMatrix = currentMatrix
      currentMatrix = nextMatrix.localCheckpoint()
      if (prevMatrix ne coboundaryMatrix) {
        prevMatrix.unpersist(false)
      }

      shouldContinueReducing = !hasPivotChangedAcc.isZero
    }

    currentMatrix
  }

  private def reducePartition(
    partition: Iterator[CoboundaryMatrixColumn]
  )(implicit
    context: FiltrationContext
  ): (Iterator[CoboundaryMatrixColumn], () => Boolean) = {
    val pivotMap        = LongMap.empty[CoboundaryMatrixColumn]
    var hasPivotChanged = false

    val reducedIterator = partition.map { col =>
      val mutableCol           = MutableCoboundaryMatrixColumn(col)
      var pOpt                 = mutableCol.pivot
      var foundDefinitivePivot = false

      while (pOpt.isDefined && !foundDefinitivePivot) {
        val currentPivot = pOpt.get
        if (pivotMap.contains(currentPivot.index)) {
          hasPivotChanged = true
          mutableCol += pivotMap(currentPivot.index)
          pOpt = mutableCol.pivot
        } else {
          ApparentPairsDetector.getBirthIfIsDeathOfApparentPair(currentPivot) match {
            case Some(birthSimplex) =>
              hasPivotChanged = true
              mutableCol += birthSimplex
              pOpt = mutableCol.pivot
            case None =>
              foundDefinitivePivot = true
          }
        }
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
          ApparentPairsDetector.getBirthIfIsDeathOfApparentPair(currentElement) match {
            case Some(birthSimplex) =>
              mutableCol += birthSimplex
            case None =>
              mutableCol.dequeueToBuffer()
          }
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

}
