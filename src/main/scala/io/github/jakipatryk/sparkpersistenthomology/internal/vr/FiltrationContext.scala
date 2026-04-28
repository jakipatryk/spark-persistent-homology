package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

private[sparkpersistenthomology] case class SparseDistanceMatrix(
  neighbors: Array[Array[Int]],
  distances: Array[Array[Float]]
)

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
    val pointsArr = pointsCloud.value
    val numPoints = pointsArr.length
    val neighbors = new Array[Array[Int]](numPoints)
    val distances = new Array[Array[Float]](numPoints)

    val tempIndices   = new Array[Int](numPoints)
    val tempDistances = new Array[Float](numPoints)

    for (i <- 0 until numPoints) {
      val pointI = pointsArr(i)
      var count  = 0

      var j = numPoints - 1
      while (j >= 0) {
        if (i != j) {
          val dist = distanceCalculator.calculateDistance(pointI, pointsArr(j))
          if (dist <= distanceThreshold) {
            tempIndices(count) = j
            tempDistances(count) = dist
            count += 1
          }
        }
        j -= 1
      }

      val nbsIndices = new Array[Int](count)
      val nbsDists   = new Array[Float](count)
      System.arraycopy(tempIndices, 0, nbsIndices, 0, count)
      System.arraycopy(tempDistances, 0, nbsDists, 0, count)

      neighbors(i) = nbsIndices
      distances(i) = nbsDists
    }

    val distanceMatrixBroadcast =
      spark.sparkContext.broadcast(SparseDistanceMatrix(neighbors, distances))

    new FiltrationContext(
      cns,
      pointsCloud,
      distanceMatrixBroadcast,
      distanceCalculator,
      distanceThreshold
    )
  }
}
