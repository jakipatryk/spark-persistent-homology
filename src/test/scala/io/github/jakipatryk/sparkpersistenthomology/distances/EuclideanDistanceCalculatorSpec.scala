package io.github.jakipatryk.sparkpersistenthomology.distances

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator.EuclideanDistanceCalculator
import org.scalatest.flatspec.AnyFlatSpec

class EuclideanDistanceCalculatorSpec extends AnyFlatSpec {

  "calculateDistance" should "return 0 when two arrays are the same" in {
    val array = Array(0.0f, 1.1f, 2.2f, 3.3f)

    val distance = EuclideanDistanceCalculator.calculateDistance(array, array)

    assert(distance == 0.0f)
  }

  "calculateDistance" should "calculate correct distance for arrays size 1" in {
    val array1 = Array(0.0f)
    val array2 = Array(2.0f)

    val distance = EuclideanDistanceCalculator.calculateDistance(array1, array2)

    assert(distance == 2.0f)
  }

  "calculateDistance" should "calculate correct distance for arrays size 2" in {
    val array1 = Array(0.0f, 1.0f)
    val array2 = Array(2.0f, -1.0f)

    val distance = EuclideanDistanceCalculator.calculateDistance(array1, array2)

    assert(distance == math.sqrt(8.0).toFloat)
  }

  "calculateDistance" should "calculate correct distance for arrays size 3" in {
    val array1 = Array(0.0f, 1.0f, 10.0f)
    val array2 = Array(2.0f, -1.0f, 13.0f)

    val distance = EuclideanDistanceCalculator.calculateDistance(array1, array2)

    assert(distance == math.sqrt(17.0).toFloat)
  }

  "calculateDistance" should "calculate correct distance for arrays size 5" in {
    val array1 = Array(0.0f, 1.0f, 10.0f, -12.2f, 10.1f)
    val array2 = Array(2.0f, -1.0f, 13.0f, -10.2f, 10.2f)

    val distance = EuclideanDistanceCalculator.calculateDistance(array1, array2)

    assert(distance == math.sqrt(21.01).toFloat)
  }

}
