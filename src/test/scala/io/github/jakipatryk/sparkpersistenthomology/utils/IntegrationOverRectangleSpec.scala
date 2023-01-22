package io.github.jakipatryk.sparkpersistenthomology.utils

import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.flatspec.AnyFlatSpec

class IntegrationOverRectangleSpec extends AnyFlatSpec {

  "computeMonteCarloIntegral" should
    "output (area * const) over square of length 1 where f is constant and bottom-left is (0, 0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val squareBottomLeftCoordinate = (0.0, 0.0)
    val squareLength = 1.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, squareBottomLeftCoordinate, squareLength, squareLength)

    assert(resultIntegral == 5.0)
  }

  "computeMonteCarloIntegral" should
    "output (area * const) over square of length 2 where f is constant and bottom-left is (0, 0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val squareBottomLeftCoordinate = (0.0, 0.0)
    val squareLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, squareBottomLeftCoordinate, squareLength, squareLength)

    assert(resultIntegral == 20.0)
  }

  "computeMonteCarloIntegral" should
    "output (area * const) over square of length 2 where f is constant and bottom-left is (5.0, 10.0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val squareBottomLeftCoordinate = (5.0, 10.0)
    val squareLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, squareBottomLeftCoordinate, squareLength, squareLength)

    assert(resultIntegral == 60.0)
  }

  "computeMonteCarloIntegral" should
    "output (area * const) over square of length 2 where f is constant and bottom-left is (-5.0, -10.0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val squareBottomLeftCoordinate = (-5.0, -10.0)
    val squareLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, squareBottomLeftCoordinate, squareLength, squareLength)

    assert(resultIntegral == 20.0)
  }

  "computeMonteCarloIntegral" should
    "output (area * const) over rectangle of length (1, 2) where f is constant and bottom-left is (0, 0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val rectangleBottomLeftCoordinate = (0.0, 0.0)
    val rectangleXLength = 1.0
    val rectangleYLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, rectangleBottomLeftCoordinate, rectangleXLength, rectangleYLength)

    assert(resultIntegral == 10.0)
  }

  "computeMonteCarloIntegral" should
    "output (area * const) over rectangle of length (1.0, 2.0) where f is constant and bottom-left is (5.0, 10.0)" in {

    val f = (x: Double, y: Double) => if (x <= 2.0 && y <= 2.0) 5.0 else 15.0
    val rectangleBottomLeftCoordinate = (5.0, 10.0)
    val rectangleXLength = 1.0
    val rectangleYLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, rectangleBottomLeftCoordinate, rectangleXLength, rectangleYLength)

    assert(resultIntegral == 30.0)
  }

  "computeMonteCarloIntegral" should
    "output good approximation of integral when f is linear and numSamples is large" in {

    val f = (x: Double, y: Double) => x + y
    val rectangleBottomLeftCoordinate = (0.0, 0.0)
    val rectangleXLength = 1.0
    val rectangleYLength = 2.0

    val resultIntegral = IntegrationOverRectangle
      .computeMonteCarloIntegral(f, rectangleBottomLeftCoordinate, rectangleXLength, rectangleYLength, 100000)

    implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.1)
    assert(resultIntegral === 3.0)
  }

}
