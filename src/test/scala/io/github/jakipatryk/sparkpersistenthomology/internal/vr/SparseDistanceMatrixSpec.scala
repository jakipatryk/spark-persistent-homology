package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparseDistanceMatrixSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  "SparseDistanceMatrix" should "correctly calculate common neighbors for intersecting neighborhoods" in {
    // Graph: 3 is connected to 2, 1, 0. 2 is connected to 1, 0. 1 is connected to 0.
    // Neighbors lists are sorted descending.
    val neighbors = Array(
      Array(3, 2, 1), // 0
      Array(3, 2, 0), // 1
      Array(3, 1, 0), // 2
      Array(2, 1, 0)  // 3
    )
    val distances = Array.fill(4)(Array.fill(3)(1.0f))
    val matrix    = SparseDistanceMatrix(neighbors, distances)

    // Intersection of neighbors of 0 and 1: {3, 2, 1} AND {3, 2, 0} = {3, 2}
    val common = matrix.getCommonNeighbors(Array(0, 1))
    common shouldBe Array(3, 2)
  }

  it should "return empty array when neighborhoods are disjoint" in {
    val neighbors = Array(
      Array(1), // 0
      Array(0), // 1
      Array(3), // 2
      Array(2)  // 3
    )
    val distances = Array.fill(4)(Array.fill(1)(1.0f))
    val matrix    = SparseDistanceMatrix(neighbors, distances)

    val common = matrix.getCommonNeighbors(Array(0, 2))
    common shouldBe Array.empty[Int]
  }

  it should "return empty array for empty input vertices" in {
    val matrix = SparseDistanceMatrix(Array.empty, Array.empty)
    matrix.getCommonNeighbors(Array.empty) shouldBe Array.empty[Int]
  }

  it should "return neighbors of the single vertex when input is a single vertex" in {
    val neighbors = Array(Array(2, 1))
    val distances = Array(Array(1.0f, 1.0f))
    val matrix    = SparseDistanceMatrix(neighbors, distances)

    matrix.getCommonNeighbors(Array(0)) shouldBe Array(2, 1)
  }

  it should "correctly apply threshold during construction" in {
    val points = Array(
      Array(0.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(0.0f, 10.0f)
    )
    val threshold       = 2.0f
    val pointsBroadcast = spark.sparkContext.broadcast(points)
    val matrix =
      SparseDistanceMatrix(
        pointsBroadcast,
        DistanceCalculator.EuclideanDistanceCalculator,
        threshold
      )

    // 0 is close to 1 (dist=1) but far from 2 (dist=10)
    matrix.neighbors(0) shouldBe Array(1)
    matrix.neighbors(1) shouldBe Array(0)
    matrix.neighbors(2) shouldBe Array.empty[Int]
  }
}
