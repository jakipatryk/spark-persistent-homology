package io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators

import io.github.jakipatryk.sparkpersistenthomology.utils.CombinatorialNumberSystemOnSteroids

import scala.util.Random

class VietorisRipsOptimisedOrchestrator(
                                         override val numberOfPartitions: Int,
                                         private val numberOfPoints: Int,
                                         private val maxSimplicesDim: Int
                                       ) extends Orchestrator {

  override val numberOfSteps: Int = maxSimplicesDim * (2 * numberOfPartitions - 1)

  private val cns = CombinatorialNumberSystemOnSteroids(numberOfPoints, maxSimplicesDim + 1)

  override def getPartitionIndexForPivot(step: Int, pivot: Long): Int = {
    val dim = currentlyProcessedDim(step)

    val dimStartIndexInclusive = cns.combinationsRunningCounts(dim)
    val dimEndIndexExclusive = cns.combinationsRunningCounts(dim + 1)

    if(pivot < dimStartIndexInclusive || pivot >= dimEndIndexExclusive)
      Random.nextInt(numberOfPartitions)
    else {
      val dimSize = dimEndIndexExclusive - dimStartIndexInclusive
      val pivotCentralized = pivot - dimStartIndexInclusive

      Math.floor(
        pivotCentralized / Math.ceil(dimSize.toFloat / numberOfPartitions)
      ).toInt
    }
  }

  override def getPartitionRowBounds(step: Int, partitionIndex: Int): Orchestrator.Bounds = {
    val dim = currentlyProcessedDim(step)

    val dimStartIndexInclusive = cns.combinationsRunningCounts(dim)
    val dimEndIndexExclusive = cns.combinationsRunningCounts(dim + 1)
    val dimSize = dimEndIndexExclusive - dimStartIndexInclusive
    val onePartitionSize = Math.ceil(dimSize.toDouble / numberOfPartitions).toLong

    val start = Orchestrator.BoundStartInclusive(dimStartIndexInclusive + partitionIndex * onePartitionSize)
    val end = Orchestrator.BoundEndInclusive(
      math.min(
        dimStartIndexInclusive + (partitionIndex + 1) * onePartitionSize - 1L,
        dimEndIndexExclusive - 1
      )
    )

    Orchestrator.Bounds(start, end)
  }

  override def getPartitionColumnBounds(step: Int, partitionIndex: Int): Orchestrator.Bounds = {
    val dim = currentlyProcessedDim(step)

    val dimColumnsStartIndexInclusive = cns.combinationsRunningCounts(dim + 1)
    val dimColumnsEndIndexExclusive = cns.combinationsRunningCounts(dim + 2)
    val dimColumnsSize = dimColumnsEndIndexExclusive - dimColumnsStartIndexInclusive
    val onePartitionSize = Math.ceil(dimColumnsSize.toDouble / numberOfPartitions).toLong
    val stepCentralized = step % (2 * numberOfPartitions - 1)

    if(stepCentralized < numberOfPartitions - 1) {
      val shift = (stepCentralized - (numberOfPartitions - partitionIndex - 1)) * onePartitionSize

      val start = Orchestrator.BoundStartInclusive(
        dimColumnsStartIndexInclusive + shift
      )
      val end = Orchestrator.BoundEndInclusive(
        math.min(
          dimColumnsStartIndexInclusive + shift + onePartitionSize - 1L,
          dimColumnsEndIndexExclusive - 1L
        )
      )
      Orchestrator.Bounds(start, end)
    } else {
      val stepShift = (stepCentralized - numberOfPartitions + 1) * onePartitionSize

      val start = Orchestrator.BoundStartInclusive(
        dimColumnsStartIndexInclusive + partitionIndex * onePartitionSize + stepShift
      )
      val end = Orchestrator.BoundEndInclusive(
        math.min(
          dimColumnsStartIndexInclusive + (partitionIndex + 1) * onePartitionSize + stepShift - 1L,
          dimColumnsEndIndexExclusive - 1L
        )
      )
      Orchestrator.Bounds(start, end)
    }
  }

  override def shouldSkipPartition(step: Int, partitionIndex: Int): Boolean = {
    val stepCentralized = step % (2 * numberOfPartitions - 1)

    if(stepCentralized < numberOfPartitions - 1)
      partitionIndex < numberOfPartitions - stepCentralized - 1
    else
      stepCentralized - numberOfPartitions + 1 + partitionIndex >= numberOfPartitions
  }

  private def currentlyProcessedDim(step: Int): Int = step / (2 * numberOfPartitions - 1)

}
