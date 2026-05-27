package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem
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

    val cns                  = CombinatorialNumberSystem(3, 4)
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
      initialSimplex = Simplex(SimplexIndex(0, context.indexPadding), 0, 0.0f),
      value = Array(Simplex(SimplexIndex(1, context.indexPadding), 1, 1.0f))
    )
    val previousDimResult: Dataset[CoboundaryMatrixColumn] = spark.createDataset(Seq(colWithPivot1))

    val numSimplicesNextDim = cns.allCombinationsCount(Simplex.dimToCombinationSize(2)) // triangles

    val result = CoboundaryMatrixConstructor.construct(1, Some(previousDimResult))

    val indices = result.map(_.initialSimplex.index).collect().toSet
    // Indices 0, 1, 2 are possible. 1 should be filtered out (clearing optimization)
    // Index 2 is in apparent pair with triangle 0.
    // Index 0 edge {1,0} has triangle {2,1,0} as cofacet. Triangle 0 is death of apparent pair with edge 2.
    // Index 0's column is no longer filtered in the constructor because it contains birth of an apparent pair cofacet.
    assert(indices.contains(SimplexIndex(0, context.indexPadding)))
    assert(!indices.contains(SimplexIndex(1, context.indexPadding)))
    assert(!indices.contains(SimplexIndex(2, context.indexPadding)))
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

    val cns                  = CombinatorialNumberSystem(3, 4)
    val broadcastCns         = sparkContext.broadcast(cns)
    val broadcastPointsCloud = sparkContext.broadcast(pointsCloud)

    implicit val context: FiltrationContext = FiltrationContext(
      broadcastCns,
      broadcastPointsCloud,
      distanceCalculator,
      distanceThreshold
    )

    val numSimplicesNextDim = cns.allCombinationsCount(Simplex.dimToCombinationSize(2))

    val result = CoboundaryMatrixConstructor.construct(1, None)

    val indices = result.map(_.initialSimplex.index).collect().toSet
    // Index 0 and 1 are no longer filtered out in the constructor.
    // Index 2 is birth of apparent pair.
    assert(indices.contains(SimplexIndex(0, context.indexPadding)))
    assert(indices.contains(SimplexIndex(1, context.indexPadding)))
    assert(!indices.contains(SimplexIndex(2, context.indexPadding)))
  }

}
