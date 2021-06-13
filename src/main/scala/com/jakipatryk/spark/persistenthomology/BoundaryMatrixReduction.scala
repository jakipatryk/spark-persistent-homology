package com.jakipatryk.spark.persistenthomology

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object BoundaryMatrixReduction {

  def reduceBlock(
                   partition: Iterator[(Key, Chain)],
                   blockColumnRange: (Long, Long),
                   blockRowRange: (Long, Long)
                 ): Iterator[(Key, Chain)] = {

    val (alreadyReduced, unreduced) = partition.span { case (key, _) => key.indexInMatrix < blockColumnRange._1 }
    val (unreducedToProcess, toReduceLater) = unreduced.span {
      case (key, _) => key.indexInMatrix <= blockColumnRange._2
    }
    val reduced = mutable.HashMap(
      alreadyReduced
      .toList
      .map { case (k, c) => (k.pivot, (k, c)) }: _*
    )
    var processedButUnreduced: List[(Key, Chain)] = Nil

    for ((key, chain) <- unreducedToProcess) {
      var currentChain = chain
      while (currentChain.pivot.nonEmpty && reduced.contains(currentChain.pivot)) {
        currentChain = currentChain + reduced(currentChain.pivot)._2
      }
      val pivot: Long = currentChain.pivot.getOrElse(-1)
      if (pivot >= blockRowRange._1 && pivot <= blockRowRange._2) {
        reduced.put(currentChain.pivot, (Key(key.indexInMatrix, currentChain.pivot), currentChain))
      } else if(currentChain.pivot.nonEmpty) {
        processedButUnreduced = (Key(key.indexInMatrix, currentChain.pivot), currentChain) :: processedButUnreduced
      }
    }

    reduced.toList.map { case (_, v) => v }.iterator ++ processedButUnreduced.iterator ++ toReduceLater
  }

  def reduceBoundaryMatrix(
                            boundaryMatrix: RDD[(Key, Chain)],
                            numOfPartitions: Int,
                            filtrationLength: Long): RDD[(Key, Chain)] = {

    val partitioner = new PivotPartitioner(numOfPartitions, filtrationLength)

    implicit val keyOrdering: Ordering[Key] = Ordering.by[Key, Long](_.indexInMatrix)

    var reducedMatrix = boundaryMatrix
      .filter { case (k, _) => k.pivot.nonEmpty }
      .repartitionAndSortWithinPartitions(partitioner)
      .cache()

    val blockRangeLength = Math.ceil(filtrationLength.toDouble / numOfPartitions).toLong

    for (step <- 0 until numOfPartitions) {
      reducedMatrix = reducedMatrix
        .mapPartitionsWithIndex {
          case (partitionIndex, partitionIterator) =>
            val columnRange = (
              partitionIndex * blockRangeLength + step * blockRangeLength,
              (partitionIndex + 1) * blockRangeLength + step * blockRangeLength - 1
            )
            val rowRange = (partitionIndex * blockRangeLength, (partitionIndex + 1) * blockRangeLength - 1)
            reduceBlock(partitionIterator, columnRange, rowRange)
        }

      reducedMatrix = reducedMatrix
        .repartitionAndSortWithinPartitions(partitioner)
    }

    reducedMatrix
  }

}
