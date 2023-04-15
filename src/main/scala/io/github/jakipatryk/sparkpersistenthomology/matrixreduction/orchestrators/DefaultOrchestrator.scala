package io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators

class DefaultOrchestrator(
                           override val numberOfPartitions: Int,
                           val filtrationLength: Long
                         ) extends Orchestrator {

  override val numberOfSteps: Int = numberOfPartitions

  private val rangeLength = Math.ceil(filtrationLength.toDouble / numberOfPartitions).toLong

  override def getPartitionIndexForPivot(step: Int, pivot: Long): Int = Math.floor(
    pivot / Math.ceil(filtrationLength.toFloat / numberOfPartitions)
  ).toInt

  override def getPartitionRowBounds(step: Int, partitionIndex: Int): Orchestrator.Bounds = {
    val start = Orchestrator.BoundStartInclusive(partitionIndex * rangeLength)
    val end = Orchestrator.BoundEndInclusive(
      math.min(
        (partitionIndex + 1) * rangeLength - 1L,
        filtrationLength - 1L
      )
    )

    Orchestrator.Bounds(start, end)
  }

  override def getPartitionColumnBounds(step: Int, partitionIndex: Int): Orchestrator.Bounds = {
    val start = Orchestrator.BoundStartInclusive(
      partitionIndex * rangeLength + step * rangeLength
    )
    val end = Orchestrator.BoundEndInclusive(
      math.min(
        (partitionIndex + 1) * rangeLength + step * rangeLength - 1L,
        filtrationLength - 1L
      )
    )

    Orchestrator.Bounds(start, end)
  }

  override def shouldSkipPartition(step: Int, partitionIndex: Int): Boolean =
    step + partitionIndex >= numberOfPartitions

}
