package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class MutableCoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "pivot"

  it should "return elements ordered first by radius (ascending priority), then index (descending priority)" in {
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

    val simplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val col     = CoboundaryMatrixColumn(simplex)
    val mutable = MutableCoboundaryMatrixColumn(col)

    val resolved = mutable.toImmutableAndDrain.valueTopEntries

    val expectedChain = List(
      Simplex(1L, 2.toByte, 1.4142135f),
      Simplex(0L, 2.toByte, 1.4142135f),
      Simplex(4L, 2.toByte, 14.142136f)
    )

    assert(resolved.toList === expectedChain)
  }

  behavior of "+="

  it should "add two columns using fast addition on valueTopEntries when fast sum >= MinTopEntries" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud        = Array.tabulate(10)(i => Array(i.toFloat, 0.0f))
    val cns                = CombinatorialNumberSystem(10, 3)
    val simplexDim: Byte   = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initial1 = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val col1     = CoboundaryMatrixColumn(initial1)
    val fullEntries1 =
      MutableCoboundaryMatrixColumn(col1).toImmutableAndDrain.valueTopEntries

    val maxEntries = CoboundaryMatrixColumn.MaxTopEntries
    val topEntries1 =
      fullEntries1.padTo(maxEntries, Simplex(998L, 2.toByte, Float.PositiveInfinity))

    val col1Truncated = CoboundaryMatrixColumn(
      initialSimplex = initial1,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries1,
      isTruncated = true
    )

    val initial2 = Simplex(index = 1L, dim = simplexDim, radius = 2.0f)
    val col2     = CoboundaryMatrixColumn(initial2)
    val fullEntries2 =
      MutableCoboundaryMatrixColumn(col2).toImmutableAndDrain.valueTopEntries
    val topEntries2 =
      fullEntries2.padTo(maxEntries, Simplex(999L, 2.toByte, Float.PositiveInfinity))

    val col2NotTruncated = CoboundaryMatrixColumn(
      initialSimplex = initial2,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries2,
      isTruncated = false
    )

    val mutable = MutableCoboundaryMatrixColumn(col1Truncated)
    mutable += col2NotTruncated
    val result = mutable.toImmutableAndDrain

    assert(result.initialSimplex === initial1)
    assert(result.simplicesAdded.toList === List(initial2))

    val expectedCount = math.min(14, maxEntries)
    val expectedTopEntries = Array(
      Simplex(2L, 2.toByte, 3.0f),
      Simplex(1L, 2.toByte, 3.0f),
      Simplex(5L, 2.toByte, 4.0f),
      Simplex(4L, 2.toByte, 4.0f),
      Simplex(11L, 2.toByte, 5.0f),
      Simplex(10L, 2.toByte, 5.0f),
      Simplex(21L, 2.toByte, 6.0f),
      Simplex(20L, 2.toByte, 6.0f),
      Simplex(36L, 2.toByte, 7.0f),
      Simplex(35L, 2.toByte, 7.0f),
      Simplex(57L, 2.toByte, 8.0f),
      Simplex(56L, 2.toByte, 8.0f),
      Simplex(85L, 2.toByte, 9.0f),
      Simplex(84L, 2.toByte, 9.0f)
    ).take(expectedCount)

    assert(result.valueTopEntries.take(expectedCount).toSeq === expectedTopEntries.toSeq)
    assert(result.isTruncated === true)
  }

  it should "fallback to heap mode when fast sum yields few entries" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud = Array(
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
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initial1 = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val col1     = CoboundaryMatrixColumn(initial1)
    val topEntries1 =
      MutableCoboundaryMatrixColumn(col1).toImmutableAndDrain.valueTopEntries

    val col1Truncated = CoboundaryMatrixColumn(
      initialSimplex = initial1,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries1,
      isTruncated = true
    )

    val initial2 = Simplex(index = 1L, dim = simplexDim, radius = 1.0f)
    val col2     = CoboundaryMatrixColumn(initial2)
    val topEntries2 =
      MutableCoboundaryMatrixColumn(col2).toImmutableAndDrain.valueTopEntries

    val col2NotTruncated = CoboundaryMatrixColumn(
      initialSimplex = initial2,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries2,
      isTruncated = false
    )

    val mutable = MutableCoboundaryMatrixColumn(col1Truncated)
    mutable += col2NotTruncated
    val result = mutable.toImmutableAndDrain

    assert(result.initialSimplex === initial1)
    assert(result.simplicesAdded.toList === List(initial2))

    val expectedTopEntries = Array(
      Simplex(2L, 2.toByte, 1.4142135f),
      Simplex(1L, 2.toByte, 1.4142135f)
    )
    assert(result.valueTopEntries === expectedTopEntries)
  }

  it should "discard invalid elements from fast sum when operands are truncated" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud        = Array.tabulate(5)(i => Array(i.toFloat, 0.0f))
    val cns                = CombinatorialNumberSystem(5, 3)
    val simplexDim: Byte   = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val maxEntries = CoboundaryMatrixColumn.MaxTopEntries

    val initial1    = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val bound1      = Simplex(100L, 2.toByte, 10.0f)
    val topEntries1 = Array(Simplex(10L, 2.toByte, 3.0f)).padTo(maxEntries, bound1)

    val col1 = CoboundaryMatrixColumn(
      initialSimplex = initial1,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries1,
      isTruncated = true
    )

    val initial2       = Simplex(index = 1L, dim = simplexDim, radius = 2.0f)
    val invalidElement = Simplex(200L, 2.toByte, 15.0f)
    val bound2         = Simplex(200L, 2.toByte, 20.0f)
    val topEntries2 = Array(Simplex(20L, 2.toByte, 4.0f), invalidElement).padTo(maxEntries, bound2)

    val col2 = CoboundaryMatrixColumn(
      initialSimplex = initial2,
      simplicesAdded = Array.empty,
      valueTopEntries = topEntries2,
      isTruncated = false
    )

    val mutable = MutableCoboundaryMatrixColumn(col1)
    mutable += col2
    val result = mutable.toImmutableAndDrain

    assert(!result.valueTopEntries.contains(invalidElement))
  }

  it should "correctly add a single birthSimplex coboundary" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud = Array(
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
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initial1 = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val col1     = CoboundaryMatrixColumn(initial1)

    val birthSimplex = Simplex(index = 1L, dim = simplexDim, radius = 1.0f)

    val mutable = MutableCoboundaryMatrixColumn(col1)
    mutable += birthSimplex
    val result = mutable.toImmutableAndDrain

    val expectedTopEntries = Array(
      Simplex(2L, 2.toByte, 1.4142135f),
      Simplex(1L, 2.toByte, 1.4142135f)
    )
    assert(result.valueTopEntries === expectedTopEntries)
    assert(result.simplicesAdded.toList === List(birthSimplex))
  }

}
