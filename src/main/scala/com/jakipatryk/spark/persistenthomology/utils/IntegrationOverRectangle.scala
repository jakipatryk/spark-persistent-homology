package com.jakipatryk.spark.persistenthomology.utils

import scala.util.Random

object IntegrationOverRectangle {

  def computeMonteCarloIntegral(
                                 f: (Double, Double) => Double,
                                 rectangleBottomLeftCoordinate: (Double, Double),
                                 rectangleXLength: Double,
                                 rectangleYLength: Double,
                                 numSamples: Int = 20
                               ): Double = {
    val rand = new Random()

    val (x, y) = rectangleBottomLeftCoordinate

    val randomPoints = for(_ <- 0 until numSamples) yield
      (x + rand.nextDouble() * rectangleXLength, y + rand.nextDouble() * rectangleYLength)
    val randomPointsFApplied = randomPoints.map(f.tupled(_))

    val rectangleArea = rectangleXLength * rectangleYLength

    rectangleArea * (randomPointsFApplied.sum / randomPointsFApplied.length)
  }

}
