package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

/** Context for Vietoris-Rips filtration calculations.
  *
  * @param cns
  *   Broadcasted Combinatorial Number System for index <-> combination conversions.
  * @param pointsCloud
  *   Broadcasted array of points.
  * @param distanceMatrix
  *   Broadcasted sparse distance matrix containing neighbor information.
  * @param distanceCalculator
  *   Calculator for point-to-point distances.
  * @param distanceThreshold
  *   Maximum distance (radius) for simplices to be included in the filtration.
  * @param indexPadding
  *   Length of the zero-padded string representation for simplex indices. Calculated based on the
  *   maximum possible index in the filtration.
  */
private[sparkpersistenthomology] case class FiltrationContext(
  cns: Broadcast[CombinatorialNumberSystem],
  distanceMatrix: Broadcast[SparseDistanceMatrix],
  distanceCalculator: DistanceCalculator,
  distanceThreshold: Float,
  indexPadding: Int
)

private[sparkpersistenthomology] object FiltrationContext {
  def apply(
    cns: Broadcast[CombinatorialNumberSystem],
    pointsCloud: Broadcast[Array[Array[Float]]],
    distanceCalculator: DistanceCalculator,
    distanceThreshold: Float
  )(implicit spark: org.apache.spark.sql.SparkSession): FiltrationContext = {
    val distanceMatrix =
      SparseDistanceMatrix(pointsCloud, distanceCalculator, distanceThreshold)
    val distanceMatrixBroadcast = spark.sparkContext.broadcast(distanceMatrix)

    val maxIndexPadding =
      cns.value.allCombinationsCount(cns.value.maxCombinationSize).toString().length

    new FiltrationContext(
      cns,
      distanceMatrixBroadcast,
      distanceCalculator,
      distanceThreshold,
      maxIndexPadding
    )
  }
}
