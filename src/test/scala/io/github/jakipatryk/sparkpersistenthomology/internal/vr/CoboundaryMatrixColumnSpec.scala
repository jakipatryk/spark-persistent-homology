package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class CoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "resolveInitialBoundary"

  it should "return max-heap of cofacets ordered first by radius (descending priority), then reversed index (ascending priority)" in {
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

    implicit val context: FiltrationContext =
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

    val simplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)

    val pq = CoboundaryMatrixColumn.resolveInitialBoundary(simplex)

    val elements = scala.collection.mutable.ListBuffer[Simplex]()
    while (pq.nonEmpty) {
      elements += pq.dequeue()
    }

    // Expected order:
    // First, highest radius: [4, 1, 0] (radius 14.14...)
    // Then, for equal radius (1.414...), smaller index first: [2, 1, 0] (index 0) before [3, 1, 0] (index 1)

    val expectedCofacets = List(
      Simplex(4L, 2.toByte, 14.142136f),
      Simplex(0L, 2.toByte, 1.4142135f),
      Simplex(1L, 2.toByte, 1.4142135f)
    )

    assert(elements.toList === expectedCofacets)
  }
}
