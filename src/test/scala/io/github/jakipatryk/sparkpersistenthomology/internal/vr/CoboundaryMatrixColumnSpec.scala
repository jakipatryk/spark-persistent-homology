package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class CoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "apply"

  it should "create a column with correct initialSimplex and value" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(5, 5)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud5),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initialSimplex = Simplex(index = BigInt(0), dim = simplexDim)
    val column         = CoboundaryMatrixColumn(initialSimplex)

    assert(column.initialSimplex === initialSimplex)

    val expectedValue = Array(
      Simplex(BigInt(1), 2, 1.4142135f)
    )
    val sortedExpectedValue = expectedValue.sorted(CoboundaryMatrixColumn.simplexFiltrationOrdering)
    assert(column.value === sortedExpectedValue)
  }

  behavior of "pivotExpression"

  it should "return -1 when value is empty" in {
    import spark.implicits._
    val pointsCloud5 = Array(Array(0.0f, 0.0f))
    val cns          = CombinatorialNumberSystem(1, 1)
    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      sparkContext.broadcast(pointsCloud5),
      DistanceCalculator.EuclideanDistanceCalculator,
      Float.PositiveInfinity
    )

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(BigInt(0), 0.toByte, 0.0f),
        value = Array.empty
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[String].collect()

    assert(result === Array(SimplexIndex(BigInt(-1), context.indexPadding).value))
  }

  it should "return the index of the first element when value is not empty" in {
    import spark.implicits._
    val pointsCloud5 = Array(Array(0.0f, 0.0f))
    val cns          = CombinatorialNumberSystem(1, 1)
    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      sparkContext.broadcast(pointsCloud5),
      DistanceCalculator.EuclideanDistanceCalculator,
      Float.PositiveInfinity
    )

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(BigInt(0), 0.toByte, 0.0f),
        value = Array(Simplex(BigInt(123), 1.toByte, 1.0f), Simplex(BigInt(456), 1.toByte, 2.0f))
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[String].collect()

    assert(result === Array(SimplexIndex(BigInt(123), context.indexPadding).value))
  }
}
