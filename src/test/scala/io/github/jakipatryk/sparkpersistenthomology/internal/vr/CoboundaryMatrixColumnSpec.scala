package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class CoboundaryMatrixColumnSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "apply"

  it should "create a column with correct initialSimplex and valueTopEntries" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(5, 3)
    val simplexDim: Byte = 1

    implicit val context: FiltrationContext =
      FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud5),
        distanceCalculator,
        Float.PositiveInfinity
      )

    val initialSimplex = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val column         = CoboundaryMatrixColumn(initialSimplex)

    assert(column.initialSimplex === initialSimplex)
    assert(column.simplicesAdded.isEmpty)

    val expectedTopEntries = Array(
      Simplex(1, 2, 1.4142135f),
      Simplex(0, 2, 1.4142135f),
      Simplex(4, 2, 14.142136f)
    )
    assert(column.valueTopEntries === expectedTopEntries)
  }

  behavior of "pivotExpression"

  it should "return -1L when valueTopEntries is empty" in {
    import spark.implicits._

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(0L, 0.toByte, 0.0f),
        simplicesAdded = Array.empty,
        valueTopEntries = Array.empty,
        isTruncated = false
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[Long].collect()

    assert(result === Array(-1L))
  }

  it should "return the index of the first element when valueTopEntries is not empty" in {
    import spark.implicits._

    val df = Seq(
      CoboundaryMatrixColumn(
        initialSimplex = Simplex(0L, 0.toByte, 0.0f),
        simplicesAdded = Array.empty,
        valueTopEntries = Array(Simplex(123L, 1.toByte, 1.0f), Simplex(456L, 1.toByte, 2.0f)),
        isTruncated = false
      )
    ).toDS()

    val result = df.select(CoboundaryMatrixColumn.pivotExpression).as[Long].collect()

    assert(result === Array(123L))
  }
}
