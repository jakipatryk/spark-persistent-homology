package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ FiltrationConfig, SharedSparkContext }
import io.github.jakipatryk.sparkpersistenthomology.internal.flag._
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class CoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "apply"

  it should "create a column with correct initialSimplex and value" in {
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

    val initialSimplex =
      Simplex(index = SimplexIndex(BigInt(0), context.indexPadding), dim = 1.toByte, radius = 0.0f)
    val column = CoboundaryMatrixColumn(initialSimplex)

    assert(column.initialSimplex === initialSimplex)

    val expectedValue = Array(
      Simplex(SimplexIndex(BigInt(1), context.indexPadding), 2.toByte, 1.4142135f)
    )
    val sortedExpectedValue = expectedValue.sorted(CoboundaryMatrixColumn.simplexFiltrationOrdering)
    assert(column.value === sortedExpectedValue)
  }

  behavior of "pivotExpression"

  it should "return -1 when value is empty" in {
    val pointsCloud = Array(Array(0.0f, 0.0f))
    val pointsDS    = spark.createDataset(pointsCloud)
    val config      = FiltrationConfig.VietorisRips()
    val cns         = CombinatorialNumberSystem(1, 1)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(SimplexIndex(BigInt(0), context.indexPadding), 0.toByte, 0.0f),
        value = Array.empty
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[String].collect()

    assert(result === Array(SimplexIndex(BigInt(-1), context.indexPadding).value))
  }

  it should "return the index of the first element when value is not empty" in {
    val pointsCloud = Array(Array(0.0f, 0.0f))
    val pointsDS    = spark.createDataset(pointsCloud)
    val config      = FiltrationConfig.VietorisRips()
    val cns         = CombinatorialNumberSystem(1, 1)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(SimplexIndex(BigInt(0), context.indexPadding), 0.toByte, 0.0f),
        value = Array(
          Simplex(SimplexIndex(BigInt(123), context.indexPadding), 1.toByte, 1.0f),
          Simplex(SimplexIndex(BigInt(456), context.indexPadding), 1.toByte, 2.0f)
        )
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[String].collect()

    assert(result === Array(SimplexIndex(BigInt(123), context.indexPadding).value))
  }
}
