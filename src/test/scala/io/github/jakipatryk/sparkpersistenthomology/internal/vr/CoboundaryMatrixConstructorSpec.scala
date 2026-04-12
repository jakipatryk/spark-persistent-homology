package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.LocalPivotChunksStatistics
import org.apache.spark.sql.Dataset

class CoboundaryMatrixConstructorSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "construct"

  it should "correctly filter out columns that should not be reduced (clearing optimization and apparent pairs)" in {
    import spark.implicits._

    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f)
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

    // dim 1: edges.
    // Combinations of size 2: {1,0}, {2,0}, {2,1}. Indices 0, 1, 2.
    // If we have a pivot at index 1 in dim 1, it should be filtered out.

    // Mock previousDimResult (dim 0 results having pivot in dim 1)
    // Simplex(1, 1) has index 1, dim 1.
    val colWithPivot1 = CoboundaryMatrixColumn(
      initialSimplex = Simplex(0, 0, 0.0f),
      simplicesAdded = Array.empty,
      valueTopEntries = Array(Simplex(1, 1, 1.0f)),
      isTruncated = false
    )
    val previousDimResult: Dataset[CoboundaryMatrixColumn] = spark.createDataset(Seq(colWithPivot1))

    val numSimplicesNextDim = cns.allCombinationsCount(Simplex.dimToCombinationSize(2)) // triangles
    val accumulator =
      new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, numSimplicesNextDim))
    sparkContext.register(accumulator)

    val result = CoboundaryMatrixConstructor.construct(1, accumulator, Some(previousDimResult))

    val indices = result.map(_.initialSimplex.index).collect().toSet
    // Indices 0, 1, 2 are possible. 1 should be filtered out (clearing optimization)
    // Index 2 is in apparent pair with triangle 0.
    assert(indices.contains(0L))
    assert(!indices.contains(1L))
    assert(!indices.contains(2L))
  }

  it should "work fine when there are no previous dim results" in {
    import spark.implicits._

    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f)
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

    val numSimplicesNextDim = cns.allCombinationsCount(Simplex.dimToCombinationSize(2))
    val accumulator =
      new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, numSimplicesNextDim))
    sparkContext.register(accumulator)

    val result = CoboundaryMatrixConstructor.construct(1, accumulator, None)

    val indices = result.map(_.initialSimplex.index).collect().toSet
    // Indices 0, 1, 2 are possible.
    // Index 2 is in apparent pair with triangle 0.
    assert(indices.contains(0L))
    assert(indices.contains(1L))
    assert(!indices.contains(2L))
  }

  it should "update the accumulator correctly" in {
    import spark.implicits._

    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val distanceThreshold  = 20.0f

    val cns                  = CombinatorialNumberSystem(4, 3)
    val broadcastCns         = sparkContext.broadcast(cns)
    val broadcastPointsCloud = sparkContext.broadcast(pointsCloud)

    implicit val context: FiltrationContext = FiltrationContext(
      broadcastCns,
      broadcastPointsCloud,
      distanceCalculator,
      distanceThreshold
    )

    val numSimplicesNextDim = cns.allCombinationsCount(Simplex.dimToCombinationSize(2))
    val accumulator =
      new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(1L, numSimplicesNextDim))
    sparkContext.register(accumulator)

    val result = CoboundaryMatrixConstructor.construct(1, accumulator, None)
    result.collect()

    val stats      = accumulator.value
    val totalCount = stats.chunks.map(_.count).sum
    val numColumns = result.count()

    assert(totalCount == numColumns)
    assert(numColumns == 3)
    assert(stats.chunks(0).count == 2)
    assert(stats.chunks(1).count == 0)
    assert(stats.chunks(2).count == 0)
    assert(stats.chunks(3).count == 1)

  }

}
