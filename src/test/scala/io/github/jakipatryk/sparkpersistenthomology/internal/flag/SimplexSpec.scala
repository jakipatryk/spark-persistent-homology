package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ FiltrationConfig, SharedSparkContext }
import io.github.jakipatryk.sparkpersistenthomology.internal.flag._
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

class SimplexSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "getFacets"

  it should "return facets ordered by CNS index in ascending order" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud)
    val config   = FiltrationConfig.VietorisRips()
    val cns      = CombinatorialNumberSystem(4, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val simplex =
      Simplex(
        index = SimplexIndex(BigInt(3), context.indexPadding),
        dim = 2.toByte,
        radius = 1.4142135f
      )
    val iterator = simplex.getFacets

    val facets = iterator.toList
    val expectedFacets = List(
      Simplex(SimplexIndex(BigInt(2), context.indexPadding), 1.toByte, 1.4142135f),
      Simplex(SimplexIndex(BigInt(4), context.indexPadding), 1.toByte, 1.0f),
      Simplex(SimplexIndex(BigInt(5), context.indexPadding), 1.toByte, 1.0f)
    )
    assert(facets === expectedFacets)
  }

  behavior of "getCofacets"

  it should "return cofacets ordered by CNS index in descending order" in {
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud5)
    val config   = FiltrationConfig.VietorisRips(distanceThreshold = Some(15.0f))
    val cns      = CombinatorialNumberSystem(5, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud5),
      config
    )

    val simplex =
      Simplex(index = SimplexIndex(BigInt(0), context.indexPadding), dim = 1.toByte, radius = 1.0f)
    val iterator = simplex.getCofacets

    val cofacets = iterator.toList
    val expectedCofacets = List(
      Simplex(SimplexIndex(BigInt(4), context.indexPadding), 2.toByte, 14.142136f),
      Simplex(SimplexIndex(BigInt(1), context.indexPadding), 2.toByte, 1.4142135f),
      Simplex(SimplexIndex(BigInt(0), context.indexPadding), 2.toByte, 1.4142135f)
    )
    assert(cofacets === expectedCofacets)
  }

  it should "not return cofacets with radius exceeding distanceThreshold" in {
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud5)
    val config   = FiltrationConfig.VietorisRips(distanceThreshold = Some(5.0f))
    val cns      = CombinatorialNumberSystem(5, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud5),
      config
    )

    val simplex =
      Simplex(index = SimplexIndex(BigInt(0), context.indexPadding), dim = 1.toByte, radius = 1.0f)
    val iterator = simplex.getCofacets

    val cofacets = iterator.toList
    assert(cofacets.length === 2)

    val expectedIndices =
      Set(
        SimplexIndex(BigInt(0), context.indexPadding),
        SimplexIndex(BigInt(1), context.indexPadding)
      )
    assert(
      cofacets.map(_.index).toSet === expectedIndices
    )
  }

  behavior of "apply"

  it should "correctly compute radius when created with apply(index, dim)" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(3.0f, 0.0f),
      Array(0.0f, 4.0f)
    )
    val pointsDS = spark.createDataset(pointsCloud)
    val config   = FiltrationConfig.VietorisRips(distanceThreshold = Some(5.0f))
    val cns      = CombinatorialNumberSystem(3, 5)

    implicit val context: FiltrationContext = FiltrationContext(
      sparkContext.broadcast(cns),
      pointsDS,
      sparkContext.broadcast(pointsCloud),
      config
    )

    val simplex = Simplex(BigInt(0), 2.toByte)

    assert(simplex.index === SimplexIndex(BigInt(0), context.indexPadding))
    assert(simplex.dim === 2.toByte)
    assert(simplex.radius === 5.0f)
  }

}
