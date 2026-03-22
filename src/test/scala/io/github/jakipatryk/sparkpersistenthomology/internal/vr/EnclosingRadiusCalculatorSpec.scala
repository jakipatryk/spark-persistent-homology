package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator.EuclideanDistanceCalculator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnclosingRadiusCalculatorSpec extends AnyFlatSpec with SharedSparkContext with Matchers {

  "computeRadius" should "return 0.0f for an empty point cloud" in {
    val spark = sparkSession
    import spark.implicits._
    val pointsCloud       = sparkSession.emptyDataset[Array[Float]]
    val broadcastedPoints = sparkContext.broadcast(Array.empty[Array[Float]])

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    radius shouldBe 0.0f
  }

  it should "return 0.0f for a single point cloud" in {
    val spark = sparkSession
    import spark.implicits._
    val localPoints       = Array(Array(1.0f, 2.0f, 3.0f))
    val pointsCloud       = sparkContext.parallelize(localPoints).toDS()
    val broadcastedPoints = sparkContext.broadcast(localPoints)

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    radius shouldBe 0.0f
  }

  it should "calculate correct enclosing radius for an equilateral triangle" in {
    val spark = sparkSession
    import spark.implicits._
    // Triangle with side length 1.0. Vertices: (0,0), (1,0), (0.5, sqrt(3)/2)
    val localPoints = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.5f, math.sqrt(3).toFloat / 2.0f)
    )
    val pointsCloud       = sparkContext.parallelize(localPoints).toDS()
    val broadcastedPoints = sparkContext.broadcast(localPoints)

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    // Max distance from any vertex to another is 1.0. Minimum of max distances is 1.0.
    radius shouldBe 1.0f +- 1e-5f
  }

  it should "calculate correct enclosing radius for a regular hexagon (more than 5 points)" in {
    val spark = sparkSession
    import spark.implicits._
    // Regular hexagon with side length 1.0, centered at origin
    val localPoints = Array(
      Array(1.0f, 0.0f),
      Array(0.5f, math.sqrt(3).toFloat / 2.0f),
      Array(-0.5f, math.sqrt(3).toFloat / 2.0f),
      Array(-1.0f, 0.0f),
      Array(-0.5f, -math.sqrt(3).toFloat / 2.0f),
      Array(0.5f, -math.sqrt(3).toFloat / 2.0f)
    )
    val pointsCloud       = sparkContext.parallelize(localPoints).toDS()
    val broadcastedPoints = sparkContext.broadcast(localPoints)

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    // For any vertex, the max distance is to the opposite vertex, which is 2 * side_length = 2.0.
    radius shouldBe 2.0f +- 1e-5f
  }

  it should "calculate correct enclosing radius for points on a straight line" in {
    val spark = sparkSession
    import spark.implicits._
    // Points: (0,0), (1,0), (2,0), (3,0)
    val localPoints = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(2.0f, 0.0f),
      Array(3.0f, 0.0f)
    )
    val pointsCloud       = sparkContext.parallelize(localPoints).toDS()
    val broadcastedPoints = sparkContext.broadcast(localPoints)

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    // Max distances:
    // from (0,0): 3.0
    // from (1,0): 2.0
    // from (2,0): 2.0
    // from (3,0): 3.0
    // Minimum of max distances is 2.0
    radius shouldBe 2.0f
  }

  it should "calculate correct enclosing radius for a star-shaped cluster" in {
    val spark = sparkSession
    import spark.implicits._
    // Center point (0,0) surrounded by points at distance 5.0
    val localPoints = Array(
      Array(0.0f, 0.0f), // center
      Array(5.0f, 0.0f),
      Array(-5.0f, 0.0f),
      Array(0.0f, 5.0f),
      Array(0.0f, -5.0f)
    )
    val pointsCloud       = sparkContext.parallelize(localPoints).toDS()
    val broadcastedPoints = sparkContext.broadcast(localPoints)

    val radius = EnclosingRadiusCalculator.computeRadius(
      pointsCloud,
      broadcastedPoints,
      EuclideanDistanceCalculator
    )

    // The center point has max distance 5.0.
    // Outer points have max distance to opposite outer point = 10.0.
    // Minimum of max distances is 5.0.
    radius shouldBe 5.0f
  }
}
