package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

private[sparkpersistenthomology] case class FiltrationContext(
  cns: Broadcast[CombinatorialNumberSystem],
  pointsCloud: Broadcast[Array[Array[Float]]],
  distanceMatrix: Broadcast[SparseDistanceMatrix],
  distanceCalculator: DistanceCalculator,
  distanceThreshold: Float
)

private[sparkpersistenthomology] object FiltrationContext {
  def apply(
    cns: Broadcast[CombinatorialNumberSystem],
    pointsCloud: Broadcast[Array[Array[Float]]],
    distanceCalculator: DistanceCalculator,
    distanceThreshold: Float
  )(implicit spark: org.apache.spark.sql.SparkSession): FiltrationContext = {
    val distanceMatrix =
      SparseDistanceMatrix(pointsCloud.value, distanceCalculator, distanceThreshold)
    val distanceMatrixBroadcast = spark.sparkContext.broadcast(distanceMatrix)

    new FiltrationContext(
      cns,
      pointsCloud,
      distanceMatrixBroadcast,
      distanceCalculator,
      distanceThreshold
    )
  }
}
