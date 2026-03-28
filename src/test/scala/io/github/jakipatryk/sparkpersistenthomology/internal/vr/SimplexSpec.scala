package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class SimplexSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "getFacets"

  it should "return facets ordered by CNS index in ascending order" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    // We have 4 points. max combination size up to 3
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 2 // max combination size is 3

    implicit val context =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        Float.PositiveInfinity
      )

    // Let's take simplex with vertices [3, 2, 1] (index 3)
    // Its facets are:
    // [2, 1] (index 2), max distance = 1.4142135f (removed 3)
    // [3, 1] (index 4), max distance = 1.0f (removed 2)
    // [3, 2] (index 5), max distance = 1.0f (removed 1)
    // They should be ordered by CNS index ascending, so [2, 1], [3, 1], then [3, 2]

    val simplex  = Simplex(index = 3L, dim = simplexDim, radius = 1.4142135f)
    val iterator = simplex.getFacets

    val facets = iterator.toList
    val expectedFacets = List(
      Simplex(2L, 1.toByte, 1.4142135f),
      Simplex(4L, 1.toByte, 1.0f),
      Simplex(5L, 1.toByte, 1.0f)
    )
    assert(facets === expectedFacets)
  }

  behavior of "getCofacets"

  it should "return cofacets ordered by CNS index in descending order" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(5, 3)
    val simplexDim: Byte = 1 // combination size 2

    implicit val context =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud5),
        distanceCalculator,
        Float.PositiveInfinity
      )

    // Simplex [1, 0] (index 0)
    // Cofacets with points 2, 3, 4
    // Point 4 -> [4, 1, 0] (index 4) radius = 14.14...
    // Point 3 -> [3, 1, 0] (index 1) radius = 1.414...
    // Point 2 -> [2, 1, 0] (index 0) radius = 1.414...
    // They should be ordered by CNS index descending, so 4, 1, then 0.

    val simplex  = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val iterator = simplex.getCofacets

    val cofacets = iterator.toList
    val expectedCofacets = List(
      Simplex(4L, 2.toByte, 14.142136f),
      Simplex(1L, 2.toByte, 1.4142135f),
      Simplex(0L, 2.toByte, 1.4142135f)
    )
    assert(cofacets === expectedCofacets)
  }

  it should "not return cofacets with radius exceeding distanceThreshold" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    // Let's add a point 4: Array(10.0f, 10.0f)
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(5, 3)
    val simplexDim: Byte = 1 // combination size 2

    // Set threshold to 5.0f, which is smaller than the distance to point 4 (~14.14f)
    implicit val context =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud5),
        distanceCalculator,
        5.0f
      )

    val simplex  = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val iterator = simplex.getCofacets

    val cofacets = iterator.toList
    // Only points 2 and 3 should form cofacets, point 4 is filtered out
    assert(cofacets.length === 2)

    val expectedIndices = Set(0L, 1L)
    assert(
      cofacets.map(_.index).toSet === expectedIndices
    )
  }
}
