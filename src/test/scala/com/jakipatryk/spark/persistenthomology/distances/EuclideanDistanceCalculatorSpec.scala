package com.jakipatryk.spark.persistenthomology.distances

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class EuclideanDistanceCalculatorSpec extends AnyFlatSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("EuclideanDistanceCalculatorSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "calculateDistance" should "return 0 when two vectors are the same" in {
    val vector = Vector(0.0, 1.1, 2.2, 3.3)

    val distance = EuclideanDistanceCalculator.calculateDistance(vector, vector)

    assert(distance == 0.0)
  }

  "calculateDistance" should "calculate correct distance for vectors size 1" in {
    val vector1 = Vector(0.0)
    val vector2 = Vector(2.0)

    val distance = EuclideanDistanceCalculator.calculateDistance(vector1, vector2)

    assert(distance == 2.0)
  }

  "calculateDistance" should "calculate correct distance for vectors size 2" in {
    val vector1 = Vector(0.0, 1.0)
    val vector2 = Vector(2.0, -1.0)

    val distance = EuclideanDistanceCalculator.calculateDistance(vector1, vector2)

    assert(distance == math.sqrt(8.0))
  }

  "calculateDistance" should "calculate correct distance for vectors size 3" in {
    val vector1 = Vector(0.0, 1.0, 10.0)
    val vector2 = Vector(2.0, -1.0, 13.0)

    val distance = EuclideanDistanceCalculator.calculateDistance(vector1, vector2)

    assert(distance == math.sqrt(17.0))
  }

  "calculateDistance" should "calculate correct distance for vectors size 5" in {
    val vector1 = Vector(0.0, 1.0, 10.0, -12.2, 10.1)
    val vector2 = Vector(2.0, -1.0, 13.0, -10.2, 10.2)

    val distance = EuclideanDistanceCalculator.calculateDistance(vector1, vector2)

    assert(distance == math.sqrt(21.01))
  }

}
