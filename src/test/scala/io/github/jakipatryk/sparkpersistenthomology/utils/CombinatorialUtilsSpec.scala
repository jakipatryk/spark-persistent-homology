package io.github.jakipatryk.sparkpersistenthomology.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CombinatorialUtilsSpec extends AnyFlatSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("CombinatorialUtilsSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "computeForAllCombinationsUpToN" should "return all 1 element combinations when n=1" in {
    val list = List(1, 3, 8, 54, 5)
    val rdd = sparkContext.parallelize(list)

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 1, (l: List[(Int, Long)]) => l.head._1)
      .collect()
    val resultCombinationIndices = result.map { case (_, indices) => indices }
    val resultCombinationValues = result.map { case (value, _) => value }

    assert(result.length == 5)
    for (value <- list) assert(resultCombinationValues contains value)
    for (index <- 0 until 5) assert(resultCombinationIndices contains List(index))
  }

  "computeForAllCombinationsUpToN" should "calculate max(5, v) when given that function (n=1)" in {
    val rdd = sparkContext.parallelize(List(1, 3, 8, 54, 5))

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 1, (l: List[(Int, Long)]) => math.max(5, l.head._1))
      .collect()
      .map { case (i, _) => i }

    assert(result.length == 5)
    assert(!(result contains 1))
    assert(!(result contains 3))
    assert(result contains 8)
    assert(result contains 54)
    assert(result contains 5)
  }

  "computeForAllCombinationsUpToN" should "return all 1 and 2 element combinations when n=2" in {
    val rdd = sparkContext.parallelize(List(1, 3, 8, 54, 5))

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 2, (l: List[(Int, Long)]) => 0)
      .collect()
      .map { case (_, indices) => indices }

    assert(result.length == 15)
    for (i <- 0 until 5) {
      assert(result contains List(i))
      for (j <- (i + 1) until 5) assert(result contains List(j, i))
    }
  }

  "computeForAllCombinationsUpToN" should "calculate sum when given that function (n=2)" in {
    val list = List(1, 3, 8, 54, 5)
    val rdd = sparkContext.parallelize(list)

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 2, (l: List[(Int, Long)]) => l.map { case (i, _) => i }.sum )
      .collect()
    val resultCombinationSums = result.map { case (value, _) => value }

    assert(result.length == 15)
    for (i <- 0 until 5) {
      assert(resultCombinationSums contains list(i))
      for (j <- (i + 1) until 5)
        assert(resultCombinationSums contains list(j) + list(i))
    }
  }

  "computeForAllCombinationsUpToN" should "return all 1, 2 and 3 element combinations when n=3" in {
    val rdd = sparkContext.parallelize(List(1, 3, 8, 54, 5))

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 3, (l: List[(Int, Long)]) => 0)
      .collect()
      .map { case (_, indices) => indices }

    assert(result.length == 25)
    for (i <- 0 until 5) {
      assert(result contains List(i))
      for (j <- (i + 1) until 5) {
        assert(result contains List(j, i))
        for (k <- (j + 1) until 5) assert(result contains List(k, j, i))
      }
    }
  }

  "computeForAllCombinationsUpToN" should "calculate sum when given that function (n=3)" in {
    val list = List(1, 3, 8, 54, 5)
    val rdd = sparkContext.parallelize(list)

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 3, (l: List[(Int, Long)]) => l.map { case (i, _) => i }.sum)
      .collect()
    val resultCombinationSums = result.map { case (value, _) => value }

    assert(result.length == 25)
    for (i <- 0 until 5) {
      assert(resultCombinationSums contains list(i))
      for (j <- (i + 1) until 5) {
        assert(resultCombinationSums contains list(j) + list(i))
        for (k <- (j + 1) until 5)
          assert(resultCombinationSums contains list(k) + list(j) + list(i))
      }
    }
  }

  "computeForAllCombinationsUpToN" should "return all 1, 2 and 3 element combinations when n=4" in {
    val rdd = sparkContext.parallelize(List(1, 3, 8, 54, 5))

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 4, (l: List[(Int, Long)]) => 0)
      .collect()
      .map { case (_, indices) => indices }

    assert(result.length == 30)
    for (i <- 0 until 5) {
      assert(result contains List(i))
      for (j <- (i + 1) until 5) {
        assert(result contains List(j, i))
        for (k <- (j + 1) until 5) {
          assert(result contains List(k, j, i))
          for (l <- (k + 1) until 5) assert(result contains List(l, k, j, i))
        }
      }
    }
  }

  "computeForAllCombinationsUpToN" should "calculate sum when given that function (n=4)" in {
    val list = List(1, 3, 8, 54, 5)
    val rdd = sparkContext.parallelize(list)

    val result = CombinatorialUtils
      .computeForAllCombinationsUpToN(rdd, 4, (l: List[(Int, Long)]) => l.map { case (i, _) => i }.sum)
      .collect()
    val resultCombinationSums = result.map { case (value, _) => value }

    assert(result.length == 30)
    for (i <- 0 until 5) {
      assert(resultCombinationSums contains list(i))
      for (j <- (i + 1) until 5) {
        assert(resultCombinationSums contains list(j) + list(i))
        for (k <- (j + 1) until 5) {
          assert(resultCombinationSums contains list(k) + list(j) + list(i))
          for (l <- (k + 1) until 5)
            assert(resultCombinationSums contains list(l) + list(k) + list(j) + list(i))
        }
      }
    }
  }

}
