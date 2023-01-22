package io.github.jakipatryk.sparkpersistenthomology.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class MinMaxBounds2DSpec extends AnyFlatSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("MinMaxBounds2DSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "SingleDimMinMaxBound" should "be creatable for reasonable numeric types" in {
    SingleDimMinMaxBound(1, 2)
    SingleDimMinMaxBound(1.0, 2.0)
    SingleDimMinMaxBound(1.0F, 2.0F)
  }

  "boundsPerKeyFromRDD" should "return empty Map when called on empty RDD" in {
    val rdd = sparkContext.parallelize(List[(Int, (Double, Double))]())

    val result = MinMaxBounds2D.boundsPerKeyFromRDD(rdd)

    assert(result.isEmpty)
  }

  "boundsPerKeyFromRDD" should "return bounds for each key in the RDD if all values are positive" in {
    val data = List((1, (2.0, 1.1)), (1, (1.2, 3.0)), (1, (1.8, 8.0)), (2, (0.0, 1.0)))
    val rdd = sparkContext.parallelize(data)

    val result = MinMaxBounds2D.boundsPerKeyFromRDD(rdd)

    assert(result contains 1)
    assert(result contains 2)
    assert(result(1) == MinMaxBounds2D(SingleDimMinMaxBound(1.2, 2.0), SingleDimMinMaxBound(1.1, 8.0)))
    assert(result(2) == MinMaxBounds2D(SingleDimMinMaxBound(0.0, 0.0), SingleDimMinMaxBound(1.0, 1.0)))
  }

  "boundsPerKeyFromRDD" should "return bounds for each key in the RDD if all values are negative" in {
    val data = List((1, (-2.0, -1.1)), (1, (-1.2, -3.0)), (1, (-1.8, -8.0)), (2, (0.0, -1.0)))
    val rdd = sparkContext.parallelize(data)

    val result = MinMaxBounds2D.boundsPerKeyFromRDD(rdd)

    assert(result contains 1)
    assert(result contains 2)
    assert(result(1) == MinMaxBounds2D(SingleDimMinMaxBound(-2.0, -1.2), SingleDimMinMaxBound(-8.0, -1.1)))
    assert(result(2) == MinMaxBounds2D(SingleDimMinMaxBound(0.0, 0.0), SingleDimMinMaxBound(-1.0, -1.0)))
  }

  "boundsPerKeyFromRDD" should "return bounds for each key in the RDD if values are both positive and negative" in {
    val data = List((1, (2.0, -1.1)), (1, (-1.2, 3.0)), (1, (-1.8, 8.0)), (2, (0.0, -1.0)), (2, (0.0, 18.0)))
    val rdd = sparkContext.parallelize(data)

    val result = MinMaxBounds2D.boundsPerKeyFromRDD(rdd)

    assert(result contains 1)
    assert(result contains 2)
    assert(result(1) == MinMaxBounds2D(SingleDimMinMaxBound(-1.8, 2.0), SingleDimMinMaxBound(-1.1, 8.0)))
    assert(result(2) == MinMaxBounds2D(SingleDimMinMaxBound(0.0, 0.0), SingleDimMinMaxBound(-1.0, 18.0)))
  }

}
