package io.github.jakipatryk.sparkpersistenthomology.matrixreduction

import io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators.Orchestrator
import io.github.jakipatryk.sparkpersistenthomology.{Chain, Key}

import scala.collection.mutable

object BoundaryMatrixReduction {

  def reduceBlock(
                   partition: Iterator[(Key, Chain)],
                   blockColumnRange: Orchestrator.Bounds,
                   blockRowRange: Orchestrator.Bounds
                 ): Iterator[(Key, Chain)] = {
    val (alreadyReduced, unreduced) = partition.span {
      case (key, _) => key.indexInMatrix < blockColumnRange.start.value
    }
    val (unreducedToProcess, toReduceLater) = unreduced.span {
      case (key, _) => key.indexInMatrix <= blockColumnRange.end.value
    }

    val reduced = new mutable.LongMap[(Key, Chain)]()
    alreadyReduced.foreach { case (k, c) => reduced(k.pivot.get) = (k, c) }

    val processedButUnreduced: Iterator[(Key, Chain)] = unreducedToProcess.flatMap {
      case (key, column) =>
        var currentColumn = column
        while (currentColumn.pivot.nonEmpty && reduced.contains(currentColumn.pivot.get)) {
          currentColumn = currentColumn + reduced(currentColumn.pivot.get)._2
        }

        val pivot: Long = currentColumn.pivot.getOrElse(-1)
        if (pivot >= blockRowRange.start.value && pivot <= blockRowRange.end.value) {
          reduced(pivot) = (Key(key.indexInMatrix, Some(pivot)), currentColumn)
          Iterator.empty
        } else if (currentColumn.pivot.nonEmpty)
          Iterator.single((Key(key.indexInMatrix, Some(pivot)), currentColumn))
        else Iterator.empty
    }

    processedButUnreduced ++
      toReduceLater ++
      reduced.iterator.map { case (_, v) => v }
  }

  def reduceBoundaryMatrix(
                            boundaryMatrix: BoundaryMatrix,
                            orchestrator: Orchestrator
                          ): BoundaryMatrix = {

    implicit val keyOrdering: Ordering[Key] = Ordering.by[Key, Long](_.indexInMatrix)

    var reducedMatrix = boundaryMatrix
      .rdd
      .filter { case (k, _) => k.pivot.nonEmpty }

    for (step <- 0 until orchestrator.numberOfSteps) {
      reducedMatrix = reducedMatrix
        .repartitionAndSortWithinPartitions(orchestrator.pivotPartitionerForStep(step))
        .mapPartitionsWithIndex {
          case (partitionIndex, partitionIterator) =>
            if (orchestrator.shouldSkipPartition(step, partitionIndex)) partitionIterator
            else {
              val columnRange = orchestrator.getPartitionColumnBounds(step, partitionIndex)
              val rowRange = orchestrator.getPartitionRowBounds(step, partitionIndex)
              reduceBlock(partitionIterator, columnRange, rowRange)
            }
        }
    }

    BoundaryMatrix(reducedMatrix)
  }

}
