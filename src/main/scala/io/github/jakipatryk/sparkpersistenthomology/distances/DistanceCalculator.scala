package io.github.jakipatryk.sparkpersistenthomology.distances

trait DistanceCalculator extends Serializable {

  def calculateDistance(v1: Vector[Double], v2: Vector[Double]): Double

}

object DistanceCalculator {

  object EuclideanDistanceCalculator extends DistanceCalculator {

    override def calculateDistance(v1: Vector[Double], v2: Vector[Double]): Double = math.sqrt(
      v1
        .zip(v2)
        .map { case (a, b) => (a - b) * (a - b) }
        .sum
    )

  }

}
