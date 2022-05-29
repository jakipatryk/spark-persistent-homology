package com.jakipatryk.spark.persistenthomology.distances

trait DistanceCalculator extends Serializable {

  def calculateDistance(v1: Vector[Double], v2: Vector[Double]): Double

}
