package io.github.jakipatryk.sparkpersistenthomology.distances

trait DistanceCalculator extends Serializable {

  def calculateDistance(v1: Array[Float], v2: Array[Float]): Float

}

object DistanceCalculator {

  object EuclideanDistanceCalculator extends DistanceCalculator {

    override def calculateDistance(v1: Array[Float], v2: Array[Float]): Float = {
      val len = v1.length
      var sum = 0.0f
      var i   = 0
      while (i < len) {
        val diff = v1(i) - v2(i)
        sum += diff * diff
        i += 1
      }
      math.sqrt(sum.toDouble).toFloat
    }

  }

}
