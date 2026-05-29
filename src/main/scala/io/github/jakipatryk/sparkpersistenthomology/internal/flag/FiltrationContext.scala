package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem
import io.github.jakipatryk.sparkpersistenthomology.FiltrationConfig
import org.apache.spark.sql.Dataset

/** Context for flag filtration calculations.
  *
  * @param cns
  *   Broadcasted Combinatorial Number System for index <-> combination conversions.
  * @param distanceMatrix
  *   Broadcasted sparse distance matrix containing neighbor information.
  * @param indexPadding
  *   Length of the zero-padded string representation for simplex indices. Calculated based on the
  *   maximum possible index in the filtration.
  */
private[sparkpersistenthomology] case class FiltrationContext(
  cns: Broadcast[CombinatorialNumberSystem],
  distanceMatrix: Broadcast[SparseDistanceMatrix],
  indexPadding: Int
)

private[sparkpersistenthomology] object FiltrationContext {
  def apply(
    cns: Broadcast[CombinatorialNumberSystem],
    pointsCloudDS: Dataset[Array[Float]],
    pointsCloud: Broadcast[Array[Array[Float]]],
    config: FiltrationConfig
  )(implicit spark: SparkSession): FiltrationContext = {
    val distanceMatrix =
      SparseDistanceMatrix(config, pointsCloudDS, pointsCloud)
    val distanceMatrixBroadcast = spark.sparkContext.broadcast(distanceMatrix)

    val maxIndexPadding =
      cns.value.allCombinationsCount(cns.value.maxCombinationSize).toString().length

    new FiltrationContext(
      cns,
      distanceMatrixBroadcast,
      maxIndexPadding
    )
  }
}
