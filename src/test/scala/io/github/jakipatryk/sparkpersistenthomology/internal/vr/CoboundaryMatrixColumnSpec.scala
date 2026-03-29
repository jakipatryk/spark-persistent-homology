package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class CoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "resolveInitialCoboundary"

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

    val pq = CoboundaryMatrixColumn.resolveInitialCoboundary(simplex)

    val elements = scala.collection.mutable.ListBuffer[Simplex]()
    while (pq.nonEmpty) {
      elements += pq.dequeue()
    }

    // Expected order:
    // First, highest radius: [4, 1, 0] (radius 14.14...)
    // Then, for equal radius (1.414...), smaller index first: [2, 1, 0] (index 0) before [3, 1, 0] (index 1)

    val expectedChain = List(
      Simplex(4L, 2.toByte, 14.142136f),
      Simplex(0L, 2.toByte, 1.4142135f),
      Simplex(1L, 2.toByte, 1.4142135f)
    )

    assert(elements.toList === expectedChain)
  }

  behavior of "resolveFullColumnValue"

  it should "return resolved initial boundary if simplicesAdded is empty" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    val pointsCloud4 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud4),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initialSimplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)

    val column = CoboundaryMatrixColumn(
      initialSimplex = initialSimplex,
      simplicesAdded = Array.empty[Simplex],
      valueTopEntries = Array.empty[Simplex]
    )

    val elements = column.resolveFullColumnValue

    val expected = CoboundaryMatrixColumn.resolveInitialCoboundary(initialSimplex).dequeueAll

    assert(elements === expected)
  }

  it should "resolve initialSimplex and one simplexAdded, adding them with elimination mod 2" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    val pointsCloud4 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud4),
        distanceCalculator,
        Float.PositiveInfinity
      )

    // Initial simplex [1, 0] (index 0). Cofacets: [2, 1, 0] (index 0), [3, 1, 0] (index 1)
    // Added simplex [2, 0] (index 1). Cofacets: [2, 1, 0] (index 0), [3, 2, 0] (index 2)
    // Expected remaining cofacets: [3, 1, 0] (index 1), [3, 2, 0] (index 2)
    // All cofacets have radius 1.4142135f.

    val initialSimplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val addedSimplex   = Simplex(index = 1L, dim = simplexDim, radius = 1.0f)

    val column = CoboundaryMatrixColumn(
      initialSimplex = initialSimplex,
      simplicesAdded = Array(addedSimplex),
      valueTopEntries = Array.empty[Simplex]
    )

    val elements = column.resolveFullColumnValue

    val expectedCofacets = Array(
      Simplex(1L, 2.toByte, 1.4142135f),
      Simplex(2L, 2.toByte, 1.4142135f)
    )

    assert(elements === expectedCofacets)
  }

  it should "resolve initialSimplex and three simplicesAdded, adding them with elimination mod 2" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    val pointsCloud4 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud4),
        distanceCalculator,
        Float.PositiveInfinity
      )

    // Initial simplex [1, 0] (index 0, rad 1.0). Cofacets: {0, 1}
    // Added 1: [2, 0] (index 1, rad 1.0). Cofacets: {0, 2}
    // Added 2: [2, 1] (index 2, rad 1.414...). Cofacets: {0, 3}
    // Added 3: [3, 0] (index 3, rad 1.414...). Cofacets: {1, 2}
    // Sum: {0, 1} + {0, 2} + {0, 3} + {1, 2} = {0, 3}
    // All cofacets have radius 1.4142135f.

    val initialSimplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val addedSimplices = Array(
      Simplex(index = 1L, dim = simplexDim, radius = 1.0f),
      Simplex(index = 2L, dim = simplexDim, radius = 1.4142135f),
      Simplex(index = 3L, dim = simplexDim, radius = 1.4142135f)
    )

    val column = CoboundaryMatrixColumn(
      initialSimplex = initialSimplex,
      simplicesAdded = addedSimplices,
      valueTopEntries = Array.empty[Simplex]
    )

    val elements = column.resolveFullColumnValue

    val expectedChain = Array(
      Simplex(0L, 2.toByte, 1.4142135f),
      Simplex(3L, 2.toByte, 1.4142135f)
    )

    assert(elements === expectedChain)
  }

  it should "resolve columns with cofacets having different radii and maintain correct order (radius descending)" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    // Using a points cloud where distances vary significantly
    val pointsCloud4 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 10.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud4),
        distanceCalculator,
        Float.PositiveInfinity
      )

    // Point Indices: 0:(0,0), 1:(1,0), 2:(0,10), 3:(10,10)
    // Distances:
    // (0,1): 1.0
    // (0,2): 10.0
    // (1,2): sqrt(1^2 + 10^2) = 10.0498...
    // (0,3): 14.142...
    // (1,3): 12.727...
    // (2,3): 10.0

    // Initial simplex [1, 0] (index 0, radius 1.0)
    // Cofacets of [1, 0]:
    // [2, 1, 0] (index 0): max(dist(2,1), dist(2,0), dist(1,0)) = max(10.0498, 10.0, 1.0) = 10.0498...
    // [3, 1, 0] (index 1): max(dist(3,1), dist(3,0), dist(1,0)) = max(12.727, 14.142, 1.0) = 14.142...

    // Added simplex [3, 2] (index 5, radius 10.0)
    // Cofacets of [3, 2]:
    // [3, 2, 0] (index 2): max(dist(3,2), dist(3,0), dist(2,0)) = max(10.0, 14.142, 10.0) = 14.142...
    // [3, 2, 1] (index 3): max(dist(3,2), dist(3,1), dist(2,1)) = max(10.0, 12.727, 10.0498) = 12.727...

    // Sum: { [2,1,0](10.0498), [3,1,0](14.142) } + { [3,2,0](14.142), [3,2,1](12.727) }
    // Cancellation:
    // [3,1,0] and [3,2,0] have DIFFERENT indices (1 and 2), but we must be careful.
    // Wait, the indices in CNS for [3, 1, 0] and [3, 2, 0] are DIFFERENT.
    // [3, 1, 0] -> index 1
    // [3, 2, 0] -> index 2
    // They don't cancel.

    // To get a cancellation, let's use:
    // Initial: [1, 0] -> Cofacets { [2, 1, 0](10.0498), [3, 1, 0](14.142) }
    // Added:   [2, 0] -> Cofacets { [2, 1, 0](10.0498), [3, 2, 0](14.142) }
    // Result:  { [3, 1, 0](14.142), [3, 2, 0](14.142) }

    val initialSimplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val addedSimplex   = Simplex(index = 1L, dim = simplexDim, radius = 10.0f)

    val column = CoboundaryMatrixColumn(
      initialSimplex = initialSimplex,
      simplicesAdded = Array(addedSimplex),
      valueTopEntries = Array.empty[Simplex]
    )

    val elements = column.resolveFullColumnValue

    // [3, 1, 0] index 1, radius 14.142136
    // [3, 2, 0] index 2, radius 14.142136
    // Radii are equal, so ordered by index ascending: index 1 before index 2.

    // Let's add another simplex to get a different radius in the result.
    // Added 2: [3, 2] -> Cofacets { [3, 2, 0](14.142), [3, 2, 1](12.727) }
    // Now [3, 2, 0] cancels.
    // Result: { [3, 1, 0](14.142), [3, 2, 1](12.727) }

    val addedSimplex2 = Simplex(index = 5L, dim = simplexDim, radius = 10.0f)
    val column2 = CoboundaryMatrixColumn(
      initialSimplex = initialSimplex,
      simplicesAdded = Array(addedSimplex, addedSimplex2),
      valueTopEntries = Array.empty[Simplex]
    )

    val elements2 = column2.resolveFullColumnValue

    val expected = Array(
      Simplex(1L, 2.toByte, 14.142136f), // radius 14.142...
      Simplex(3L, 2.toByte, 13.453624f)  // radius 13.453... (max(dist(3,2), dist(3,1), dist(2,1)))
    )

    assert(elements2 === expected)
  }
}
