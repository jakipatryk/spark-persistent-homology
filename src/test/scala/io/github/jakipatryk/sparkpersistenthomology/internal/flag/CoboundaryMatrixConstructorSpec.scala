package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ FiltrationConfig, SharedSparkContext }
import io.github.jakipatryk.sparkpersistenthomology.internal.flag._
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem
import org.apache.spark.sql.Dataset

class CoboundaryMatrixConstructorSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "construct"

  it should "correctly filter out columns that should not be reduced (clearing optimization and apparent pairs)" in {

    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f)
    )
    val pointsCloudDS = spark.createDataset(pointsCloud)
    val config        = FiltrationConfig.VietorisRips()
    val cns           = CombinatorialNumberSystem(3, 4)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsCloudDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    // dim 1: edges.
    // Combinations of size 2: {1,0}, {2,0}, {2,1}. Indices 0, 1, 2.
    // If we have a pivot at index 1 in dim 1, it should be filtered out.

    // Mock previousDimResult (dim 0 results having pivot in dim 1)
    // Simplex(1, 1) has index 1, dim 1.
    val colWithPivot1 = CoboundaryMatrixColumn(
      initialSimplex = Simplex(SimplexIndex(0, context.indexPadding), 0.toByte, 0.0f),
      value = Array(Simplex(SimplexIndex(1, context.indexPadding), 1.toByte, 1.0f))
    )
    val previousDimResult: Dataset[CoboundaryMatrixColumn] = spark.createDataset(Seq(colWithPivot1))

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

    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f)
    )
    val pointsCloudDS = spark.createDataset(pointsCloud)
    val config        = FiltrationConfig.VietorisRips()
    val cns           = CombinatorialNumberSystem(3, 4)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsCloudDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val result = CoboundaryMatrixConstructor.construct(1, None)

    val indices = result.map(_.initialSimplex.index).collect().toSet
    // Index 0 and 1 are no longer filtered out in the constructor.
    // Index 2 is birth of apparent pair.
    assert(indices.contains(SimplexIndex(0, context.indexPadding)))
    assert(indices.contains(SimplexIndex(1, context.indexPadding)))
    assert(!indices.contains(SimplexIndex(2, context.indexPadding)))
  }

}
