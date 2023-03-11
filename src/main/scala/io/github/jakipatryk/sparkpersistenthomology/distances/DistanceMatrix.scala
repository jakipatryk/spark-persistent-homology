package io.github.jakipatryk.sparkpersistenthomology.distances

import io.github.jakipatryk.sparkpersistenthomology.PointsCloud
import io.github.jakipatryk.sparkpersistenthomology.utils.LocalMatrix

/** Stores distances between any two points. */
case class DistanceMatrix private(matrix: LocalMatrix[Double]) extends AnyVal {

  def getDistance(pointAIndex: Int, pointBIndex: Int): Double = matrix(pointAIndex, pointBIndex)

  def numberOfPoints: Int = matrix.n

}

object DistanceMatrix {

  /**
   * Creates DistanceMatrix from a PointsCloud.
   * PointsCloud must be small enough so that if it has N elements, then N*N elements can be stored on each node.
   *
   * @param pointsCloud PointsCloud for which DistanceMatrix should be computed
   * @param distanceCalculator An object used to calculate distance in a given metric space
   * @return computed DistanceMatrix
   */
  def fromPointsCloud(
                       pointsCloud: PointsCloud,
                       distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
                     ): DistanceMatrix = {
    val points = pointsCloud.rdd.collect()
    val n = points.length

    val matrix = LocalMatrix.zero[Double](n, n)
    for {
      i <- 0 until n
      j <- 0 until n
    } matrix(i, j) = distanceCalculator.calculateDistance(points(i), points(j))

    new DistanceMatrix(matrix)
  }


}
