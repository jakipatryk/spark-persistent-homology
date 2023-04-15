package io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators

import io.github.jakipatryk.sparkpersistenthomology.Key
import org.apache.spark.Partitioner

abstract class Orchestrator extends Serializable {
  import Orchestrator._

  val numberOfSteps: Int

  val numberOfPartitions: Int

  def getPartitionIndexForPivot(step: Int, pivot: Long): Int

  def getPartitionRowBounds(step: Int, partitionIndex: Int): Bounds

  def getPartitionColumnBounds(step: Int, partitionIndex: Int): Bounds

  def shouldSkipPartition(step: Int, partitionIndex: Int): Boolean

  def pivotPartitionerForStep(step: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = numberOfPartitions

    override def getPartition(key: Any): Int = key match {
      case Key(_, Some(pivot)) => getPartitionIndexForPivot(step, pivot)
    }
  }
}

private[sparkpersistenthomology] object Orchestrator {

  case class BoundStartInclusive(value: Long) extends AnyVal

  case class BoundEndInclusive(value: Long) extends AnyVal

  case class Bounds(start: BoundStartInclusive, end: BoundEndInclusive)

}
