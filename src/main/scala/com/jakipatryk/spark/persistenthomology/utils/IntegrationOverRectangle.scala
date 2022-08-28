package com.jakipatryk.spark.persistenthomology.utils

import scala.util.Random

object IntegrationOverRectangle {

  def computeMonteCarloIntegral(f: (Double, Double) => Double)
                               (rectangleBottomLeftCoordinate: (Double, Double),
                                rectangleXLength: Double,
                                rectangleYLength: Double,
                                numSamples: Int = 20
                               ): Double = {
    val rand = new Random()

    val x = rectangleBottomLeftCoordinate._1
    val y = rectangleBottomLeftCoordinate._2

    val randomPoints = for(_ <- 0 until numSamples) yield
      (x + rand.nextDouble() * rectangleXLength, y + rand.nextDouble() * rectangleYLength)
    val randomPointsFApplied = randomPoints.map(p => f(p._1, p._2))

    val rectangleArea = rectangleXLength * rectangleYLength

    rectangleArea * (randomPointsFApplied.sum / randomPointsFApplied.length)
  }

}
