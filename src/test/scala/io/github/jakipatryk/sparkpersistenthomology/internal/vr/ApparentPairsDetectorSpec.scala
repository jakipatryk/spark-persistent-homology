package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class ApparentPairsDetectorSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "isInZeroApparentPair"

  it should "correctly identify an apparent pair in a simple triangle and ignore other edges" in {
    // Triangle (0, 1, 2)
    // Points at:
    // 0: (0, 0.1)
    // 1: (-0.5, 0)
    // 2: (0.5, 0)
    // Radii:
    // dist(1, 2) = 1.0 (Longest edge)
    // dist(0, 1) = sqrt(0.5^2 + 0.1^2) = sqrt(0.26) ≈ 0.51
    // dist(0, 2) = sqrt(0.5^2 + 0.1^2) = sqrt(0.26) ≈ 0.51
    // Triangle (0, 1, 2) radius = 1.0

    val pointsCloud = Array(
      Array(0.0f, 0.1f),
      Array(-0.5f, 0.0f),
      Array(0.5f, 0.0f)
    )
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val distanceThreshold  = 2.0f

    val cns                  = CombinatorialNumberSystem(3, 3)
    val broadcastCns         = sparkContext.broadcast(cns)
    val broadcastPointsCloud = sparkContext.broadcast(pointsCloud)

    implicit val context: FiltrationContext = FiltrationContext(
      broadcastCns,
      broadcastPointsCloud,
      distanceCalculator,
      distanceThreshold
    )

    // CNS Indices for edges (combinations of size 2):
    // {1, 0} -> binom(1, 2) + binom(0, 1) = 0 + 0 = 0
    // {2, 0} -> binom(2, 2) + binom(0, 1) = 1 + 0 = 1
    // {2, 1} -> binom(2, 2) + binom(1, 1) = 1 + 1 = 2
    // Longest edge is {2, 1} with index 2.
    val edge01 = Simplex(0, 1, 0.50990195f) // {1, 0}
    val edge02 = Simplex(1, 1, 0.50990195f) // {2, 0}
    val edge12 = Simplex(2, 1, 1.0f)        // {2, 1}

    // CNS Index for triangle (combination of size 3):
    // {2, 1, 0} -> binom(2, 3) + binom(1, 2) + binom(0, 1) = 0 + 0 + 0 = 0
    val triangle = Simplex(0, 2, 1.0f)

    // Edge {2, 1} (index 2) and Triangle {2, 1, 0} (index 0) should be an apparent pair
    assert(ApparentPairsDetector.isInZeroApparentPair(edge12))
    assert(ApparentPairsDetector.isInZeroApparentPair(triangle))

    // Other edges should NOT be in apparent pair
    assert(!ApparentPairsDetector.isInZeroApparentPair(edge01))
    assert(!ApparentPairsDetector.isInZeroApparentPair(edge02))
  }

  it should "correctly check two triangles connected by a common edge" in {
    // Triangle 1: {2, 1, 0}
    // Triangle 2: {3, 2, 1}
    // Points:
    // 0: (0, 0.1)
    // 1: (-0.5, 0)
    // 2: (0.5, 0)
    // 3: (0, -2.0)

    // T1: {2, 1, 0}. Edges: {1, 0} r=0.51, {2, 0} r=0.51, {2, 1} r=1.0. T1 r=1.0.
    // T2: {3, 2, 1}. Edges: {2, 1} r=1.0, {3, 1} r=2.06, {3, 2} r=2.06. T2 r=2.06.

    val pointsCloud = Array(
      Array(0.0f, 0.1f),  // 0
      Array(-0.5f, 0.0f), // 1
      Array(0.5f, 0.0f),  // 2
      Array(0.0f, -2.0f)  // 3
    )
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val distanceThreshold  = 5.0f

    val cns                  = CombinatorialNumberSystem(4, 3)
    val broadcastCns         = sparkContext.broadcast(cns)
    val broadcastPointsCloud = sparkContext.broadcast(pointsCloud)

    implicit val context: FiltrationContext = FiltrationContext(
      broadcastCns,
      broadcastPointsCloud,
      distanceCalculator,
      distanceThreshold
    )

    // Edges CNS Indices:
    // {1, 0} -> 0
    // {2, 0} -> 1
    // {2, 1} -> 2
    // {3, 0} -> binom(3, 2) + binom(0, 1) = 3 + 0 = 3
    // {3, 1} -> binom(3, 2) + binom(1, 1) = 3 + 1 = 4
    // {3, 2} -> binom(3, 2) + binom(2, 1) = 3 + 2 = 5

    // Triangles CNS Indices:
    // {2, 1, 0} -> 0
    // {3, 1, 0} -> binom(3, 3) + binom(1, 2) + binom(0, 1) = 1 + 0 + 0 = 1
    // {3, 2, 0} -> binom(3, 3) + binom(2, 2) + binom(0, 1) = 1 + 1 + 0 = 2
    // {3, 2, 1} -> binom(3, 3) + binom(2, 2) + binom(1, 1) = 1 + 1 + 1 = 3

    val edge01 = Simplex(0, 1, 0.50990195f)
    val edge02 = Simplex(1, 1, 0.50990195f)
    val edge12 = Simplex(2, 1, 1.0f)
    val edge13 = Simplex(4, 1, 2.0615528f)
    val edge23 = Simplex(5, 1, 2.0615528f)

    val triangle1 = Simplex(0, 2, 1.0f)
    val triangle2 = Simplex(3, 2, 2.0615528f)

    // Check T1 and its apparent pair edge {2, 1}
    assert(ApparentPairsDetector.isInZeroApparentPair(edge12))
    assert(ApparentPairsDetector.isInZeroApparentPair(triangle1))

    // Check T2 and its potential apparent pair edge.
    // Facets of T2: {2, 1} r=1.0, {3, 1} r=2.06, {3, 2} r=2.06.
    // First facet of T2 with r=2.06 is {3, 1} (index 4).
    // Cofacets of {3, 1}: {3, 1, 0} (index 1), {3, 2, 1} (index 3).
    // dist({3, 1, 0}) = max(dist(3,1)=2.06, dist(3,0)=2.1, dist(1,0)=0.51) = 2.1
    // dist({3, 2, 1}) = 2.06.
    // So {3, 1} r=2.06 cofacets: index 1 (r=2.1), index 3 (r=2.06).
    // First cofacet of {3, 1} with radius 2.06 is triangle2 (index 3).
    assert(ApparentPairsDetector.isInZeroApparentPair(edge13))
    assert(ApparentPairsDetector.isInZeroApparentPair(triangle2))

    // Edge {3, 2} (index 5) is also a facet of T2 with r=2.06, but it's the SECOND such facet.
    assert(!ApparentPairsDetector.isInZeroApparentPair(edge23))

    // Other edges
    assert(!ApparentPairsDetector.isInZeroApparentPair(edge01))
    assert(!ApparentPairsDetector.isInZeroApparentPair(edge02))
  }

}
