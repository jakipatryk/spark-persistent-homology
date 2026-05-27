package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ FiltrationConfig, SharedSparkContext }
import io.github.jakipatryk.sparkpersistenthomology.internal.flag._
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class MutableCoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "pivot"

  it should "return elements ordered first by radius (ascending priority), then index (descending priority)" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud)
    val config   = FiltrationConfig.VietorisRips()
    val cns      = CombinatorialNumberSystem(5, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val simplexDim: Byte = 1
    val simplex =
      Simplex(
        index = SimplexIndex(BigInt(0), context.indexPadding),
        dim = simplexDim,
        radius = 1.0f
      )
    val col     = CoboundaryMatrixColumn(simplex)
    val mutable = MutableCoboundaryMatrixColumn(col)

    val resolved = mutable.toImmutableAndDrain.value

    val expectedChain = List(
      Simplex(SimplexIndex(BigInt(1), context.indexPadding), 2.toByte, 1.4142135f)
    ).sorted(CoboundaryMatrixColumn.simplexFiltrationOrdering)

    assert(resolved.toList === expectedChain)
  }

  behavior of "+="

  it should "add two columns correctly using full values" in {
    val pointsCloud = Array.tabulate(10)(i => Array(i.toFloat, 0.0f))
    val pointsDS    = spark.createDataset(pointsCloud)
    val config      = FiltrationConfig.VietorisRips()
    val cns         = CombinatorialNumberSystem(10, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val simplexDim: Byte = 1
    val initial1 =
      Simplex(
        index = SimplexIndex(BigInt(0), context.indexPadding),
        dim = simplexDim,
        radius = 1.0f
      )
    val col1 = CoboundaryMatrixColumn(initial1)

    val initial2 =
      Simplex(
        index = SimplexIndex(BigInt(1), context.indexPadding),
        dim = simplexDim,
        radius = 2.0f
      )
    val col2 = CoboundaryMatrixColumn(initial2)

    val mutable = MutableCoboundaryMatrixColumn(col1)
    mutable += col2
    val result = mutable.toImmutableAndDrain

    assert(result.value.length === 1)
    assert(
      result.value.toSeq.contains(
        Simplex(SimplexIndex(BigInt(2), context.indexPadding), 2.toByte, 3.0f)
      )
    )
  }

  it should "add birth simplex correctly" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud)
    val config   = FiltrationConfig.VietorisRips()
    val cns      = CombinatorialNumberSystem(4, 4)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val simplexDim: Byte = 1
    val initial1 =
      Simplex(
        index = SimplexIndex(BigInt(0), context.indexPadding),
        dim = simplexDim,
        radius = 1.0f
      )
    val col1 = CoboundaryMatrixColumn(initial1)

    val birthSimplex =
      Simplex(
        index = SimplexIndex(BigInt(1), context.indexPadding),
        dim = simplexDim,
        radius = 1.0f
      )

    val mutable = MutableCoboundaryMatrixColumn(col1)
    mutable += birthSimplex
    val result = mutable.toImmutableAndDrain

    assert(result.value.length === 2)
    assert(
      result.value.toSeq.contains(
        Simplex(SimplexIndex(BigInt(2), context.indexPadding), 2.toByte, 1.4142135f)
      )
    )
    assert(
      result.value.toSeq.contains(
        Simplex(SimplexIndex(BigInt(1), context.indexPadding), 2.toByte, 1.4142135f)
      )
    )
  }
}
