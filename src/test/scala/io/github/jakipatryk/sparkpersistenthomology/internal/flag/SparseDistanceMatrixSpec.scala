package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import io.github.jakipatryk.sparkpersistenthomology.{ FiltrationConfig, SharedSparkContext }
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparseDistanceMatrixSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  import spark.implicits._

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
    val config          = FiltrationConfig.VietorisRips(distanceThreshold = Some(threshold))

    val matrix =
      SparseDistanceMatrix(
        config,
        spark.createDataset(points),
        pointsBroadcast
      )

    // 0 is close to 1 (dist=1) but far from 2 (dist=10)
    matrix.neighbors(0) shouldBe Array(1)
    matrix.neighbors(1) shouldBe Array(0)
    matrix.neighbors(2) shouldBe Array.empty[Int]
  }

  it should "correctly construct mutual k-nearest neighbors matrix (k=1)" in {
    val points = Array(
      Array(0.0f, 0.0f), // 0
      Array(0.0f, 1.0f), // 1
      Array(0.0f, 3.0f), // 2
      Array(0.0f, 6.0f)  // 3
    )
    val pointsBroadcast = spark.sparkContext.broadcast(points)
    val config          = FiltrationConfig.NearestNeighbors(k = 1)

    val matrix = SparseDistanceMatrix(
      config,
      spark.createDataset(points),
      pointsBroadcast
    )

    // kNN(0) = {1}, kNN(1) = {0} -> mutual
    // kNN(2) = {1}, kNN(3) = {2} -> not mutual with 1 and 2 respectively
    matrix.neighbors(0) shouldBe Array(1)
    matrix.neighbors(1) shouldBe Array(0)
    matrix.neighbors(2) shouldBe Array.empty[Int]
    matrix.neighbors(3) shouldBe Array.empty[Int]
  }

  it should "correctly construct mutual k-nearest neighbors matrix (k=2)" in {
    val points = Array(
      Array(0.0f, 0.0f), // 0
      Array(0.0f, 1.0f), // 1
      Array(0.0f, 2.5f), // 2
      Array(0.0f, 6.0f)  // 3
    )
    val pointsBroadcast = spark.sparkContext.broadcast(points)
    val config          = FiltrationConfig.NearestNeighbors(k = 2)

    val matrix = SparseDistanceMatrix(
      config,
      spark.createDataset(points),
      pointsBroadcast
    )

    // kNN(0) = {1, 2}, kNN(1) = {0, 2}, kNN(2) = {1, 0}, kNN(3) = {2, 1}
    // mutuals: 0-1, 0-2, 1-2. 3 is not mutual with 1 or 2.
    matrix.neighbors(0) shouldBe Array(2, 1)
    matrix.neighbors(1) shouldBe Array(2, 0)
    matrix.neighbors(2) shouldBe Array(1, 0)
    matrix.neighbors(3) shouldBe Array.empty[Int]
  }

  it should "correctly construct mutual k-nearest neighbors matrix (k >= numPoints - 1)" in {
    val points = Array(
      Array(0.0f, 0.0f), // 0
      Array(0.0f, 1.0f), // 1
      Array(0.0f, 2.5f)  // 2
    )
    val pointsBroadcast = spark.sparkContext.broadcast(points)
    val config          = FiltrationConfig.NearestNeighbors(k = 10)

    val matrix = SparseDistanceMatrix(
      config,
      spark.createDataset(points),
      pointsBroadcast
    )

    // All pairs should be connected, sorted descending
    matrix.neighbors(0) shouldBe Array(2, 1)
    matrix.neighbors(1) shouldBe Array(2, 0)
    matrix.neighbors(2) shouldBe Array(1, 0)
  }

  it should "correctly enforce symmetrization for k=5 on a larger set of points" in {
    val points = Array(
      Array(0.0f, 0.0f),  // 0
      Array(0.0f, 1.0f),  // 1
      Array(0.0f, 2.0f),  // 2
      Array(0.0f, 3.0f),  // 3
      Array(0.0f, 4.0f),  // 4
      Array(0.0f, 5.0f),  // 5
      Array(0.0f, 20.0f), // 6
      Array(0.0f, 21.0f), // 7
      Array(0.0f, 22.0f), // 8
      Array(0.0f, 23.0f)  // 9
    )
    val pointsBroadcast = spark.sparkContext.broadcast(points)
    val config          = FiltrationConfig.NearestNeighbors(k = 5)

    val matrix = SparseDistanceMatrix(
      config,
      spark.createDataset(points),
      pointsBroadcast
    )

    // Points 0-5 are dense, points 6-9 are dense.
    // For point 6, its 5 nearest neighbors are 7, 8, 9, 5, 4.
    // But for point 5, its 5 nearest neighbors are 4, 3, 2, 1, 0.
    // Point 5's distances: to 4(1.0), to 3(2.0), to 2(3.0), to 1(4.0), to 0(5.0), to 6(15.0).
    // So 6 is NOT in 5's kNN.
    // Hence, the edge between 5 and 6 should be removed by symmetrization.

    matrix.neighbors(5) should not contain 6
    matrix.neighbors(6) should not contain 5
    matrix.neighbors(6) should contain allOf (7, 8, 9)
  }
}
