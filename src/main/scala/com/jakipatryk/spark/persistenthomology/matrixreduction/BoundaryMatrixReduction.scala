package com.jakipatryk.spark.persistenthomology.matrixreduction

import com.jakipatryk.spark.persistenthomology.{Chain, Key}

import scala.collection.mutable

object BoundaryMatrixReduction {

  def reduceBlock(
                   partition: Iterator[(Key, Chain)],
                   blockColumnRange: (Long, Long),
                   blockRowRange: (Long, Long)
                 ): Iterator[(Key, Chain)] = {

    val (alreadyReduced, unreduced) = partition.span {
      case (key, _) => key.indexInMatrix < blockColumnRange._1
    }
    val (unreducedToProcess, toReduceLater) = unreduced.span {
      case (key, _) => key.indexInMatrix <= blockColumnRange._2
    }

    val reduced = mutable.HashMap(
      alreadyReduced
        .toSeq
        .map { case (k, c) => (k.pivot, (k, c)) }: _*
    )

    val processedButUnreduced: Iterator[(Key, Chain)] = unreducedToProcess.flatMap {
      case (key, column) =>
        var currentColumn = column
        while (currentColumn.pivot.nonEmpty && reduced.contains(currentColumn.pivot)) {
          currentColumn = currentColumn + reduced(currentColumn.pivot)._2
        }

        val pivot: Long = currentColumn.pivot.getOrElse(-1)
        if (pivot >= blockRowRange._1 && pivot <= blockRowRange._2) {
          reduced
            .put(Some(pivot), (Key(key.indexInMatrix, Some(pivot)), currentColumn))
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
                            numOfPartitions: Int,
                            boundaryMatrixInitialLength: Long
                          ): BoundaryMatrix = {

    val partitioner = new PivotPartitioner(numOfPartitions, boundaryMatrixInitialLength)

    implicit val keyOrdering: Ordering[Key] = Ordering.by[Key, Long](_.indexInMatrix)

    var reducedMatrix = boundaryMatrix
      .filter { case (k, _) => k.pivot.nonEmpty }
      .repartitionAndSortWithinPartitions(partitioner)

    val blockRangeLength = Math.ceil(boundaryMatrixInitialLength.toDouble / numOfPartitions).toLong

    for (step <- 0 until numOfPartitions) {
      reducedMatrix = reducedMatrix
        .mapPartitionsWithIndex {
          case (partitionIndex, partitionIterator) =>
            if (numOfPartitions - partitionIndex < step) partitionIterator
            else {
              val columnRange = (
                partitionIndex * blockRangeLength + step * blockRangeLength,
                (partitionIndex + 1) * blockRangeLength + step * blockRangeLength - 1
              )
              val rowRange = (partitionIndex * blockRangeLength, (partitionIndex + 1) * blockRangeLength - 1)
              reduceBlock(partitionIterator, columnRange, rowRange)
            }
        }

      reducedMatrix = reducedMatrix
        .repartitionAndSortWithinPartitions(partitioner)
    }

    reducedMatrix
  }

}
