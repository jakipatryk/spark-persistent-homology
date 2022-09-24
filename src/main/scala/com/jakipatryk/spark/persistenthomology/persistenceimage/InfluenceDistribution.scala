package com.jakipatryk.spark.persistenthomology.persistenceimage

abstract class InfluenceDistribution extends Serializable {

  /**
   * Computes influence of `center` to `point`.
   */
  def compute(center: (Double, Double), point: (Double, Double)): Double

}

object InfluenceDistribution {

  class GaussianInfluenceDistribution(varianceX: Double, varianceY: Double) extends InfluenceDistribution {

    override def compute(center: (Double, Double), point: (Double, Double)): Double = {
      val varianceXSquareRoot = math.sqrt(varianceX)
      val varianceYSquareRoot = math.sqrt(varianceY)
      val (x, y) = point
      val (meanX, meanY) = center
      val xDistance = (x - meanX) * (x - meanX)
      val yDistance = (y - meanY) * (y - meanY)

      (1 / (2 * math.Pi * varianceXSquareRoot * varianceYSquareRoot)) *
        math.exp((-1 / (2 * varianceX)) * xDistance + (-1 / (2 * varianceY)) * yDistance)
    }

  }

}
