package com.jakipatryk.spark.persistenthomology.persistenceimage

import com.jakipatryk.spark.persistenthomology.{Infinity, PersistencePair}
import com.jakipatryk.spark.persistenthomology.persistenceimage.PersistenceImage.{Birth, Persistence}
import com.jakipatryk.spark.persistenthomology.utils.SingleDimMinMaxBound
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.Source

class PersistenceImageSpec extends AnyFlatSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("PersistenceImageSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }


  behavior of "fromPersistencePairs"

  val birthDeathPairs: List[PersistencePair] = List(
    PersistencePair(0.0, Left(10.0), 0),
    PersistencePair(1.0, Left(2.3), 0),
    PersistencePair(0.2, Left(18.6), 0),
    PersistencePair(0.0, Left(6.2), 0),
    PersistencePair(8.0, Left(15.0), 0),
    PersistencePair(0.0, Right(Infinity), 0),
    PersistencePair(1.2, Left(11.2), 1),
    PersistencePair(2.3, Left(5.0), 1),
    PersistencePair(12.1, Left(12.5), 1)
  )

  def birthDeathPairsRDD: RDD[PersistencePair] = sparkContext.parallelize(birthDeathPairs)

  def imagesAlmostEqual(
                         image1: Vector[Vector[Double]],
                         image2: Vector[Vector[Double]],
                         tolerance: Double = 1e-3f
                       ): Boolean = {
    implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(tolerance)
    image1
      .zip(image2)
      .forall {
        case (v1, v2) =>
          v1.zip(v2).forall(p => p._1 === p._2)
      }
  }

  def loadImageVectorFromFile(path: String): Vector[Vector[Double]] = {
    val source = Source
      .fromURL(getClass.getResource(path))
    val image = source
      .mkString
      .split("\n")
      .map(s => s.split(",").map(_.toDouble).toVector)
      .toVector
    source.close()
    image
  }

  it should
    "output persistence image close in values to ones calculated by scikit-tda with the same settings (single dim)" in {
    val dimensionsToInclude = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.0), maxBirth = Some(12.1), minPersistence = Some(0.0), maxPersistence = Some(12.1)
        )
    )

    val result = PersistenceImage.fromPersistencePairsGaussian(birthDeathPairsRDD, dimensionsToInclude , 100, 100, 1.0)
    val fromScikitTDA = loadImageVectorFromFile("/persistenceimage/image-single-dim-scikit-tda.csv")

    assert(result.dims == Set(0))
    assert(result.image.size == 100)
    assert(result.image(0).size == 100)
    assert(imagesAlmostEqual(fromScikitTDA, result.image))
  }

  it should
    "output persistence image close in values to ones calculated by scikit-tda with the same settings (multi dim)" in {
    val dimensionsToInclude = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.0), maxBirth = Some(12.1), minPersistence = Some(0.0), maxPersistence = Some(12.1)
        ),
      1 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.0), maxBirth = Some(12.1), minPersistence = Some(0.0), maxPersistence = Some(12.1)
        )
    )

    val result = PersistenceImage
      .fromPersistencePairsGaussian(birthDeathPairsRDD, dimensionsToInclude, 100, 100, 1.0)
    val fromScikitTDA = loadImageVectorFromFile("/persistenceimage/image-multi-dim-scikit-tda.csv")

    assert(result.dims == Set(0, 1))
    assert(result.image.size == 200)
    assert(result.image(0).size == 100)
    assert(imagesAlmostEqual(fromScikitTDA, result.image))
  }


  behavior of "calculatePixelSizesPerDim"

  val birthPersistencePairs: List[(Int, (Birth, Persistence))] = List(
    (0, (Birth(0.0), Persistence(10.0))),
    (0, (Birth(1.0), Persistence(1.3))),
    (0, (Birth(0.2), Persistence(18.4))),
    (0, (Birth(0.0), Persistence(6.2))),
    (0, (Birth(0.0), Persistence(10.0))),
    (2, (Birth(1.2), Persistence(10.0))),
    (2, (Birth(454.2), Persistence(3.0))),
    (2, (Birth(12.1), Persistence(0.4)))
  )

  def birthPersistencePairsRDD(dims: Set[Int]): RDD[(Int, (Birth, Persistence))] =
    sparkContext
      .parallelize(birthPersistencePairs)
      .filter(dims contains _._1)

  it should "calculate correct pixel size when all bounds are provided by config (single dim, square image)" in {
    val dims = Set(0)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixels = 100
    val dimensions = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.1), maxBirth = Some(2.3), minPersistence = Some(0.0), maxPersistence = Some(18.4)
        )
    )

    val result = PersistenceImage.calculatePixelSizesPerDim(rdd, numberOfPixels, numberOfPixels, dimensions)

    assert(result.size == 1)
    assert(result contains 0)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((2.3 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 100.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 2.3))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
  }

  it should "calculate correct pixel size when all bounds are provided by config (single dim, rectangular image)" in {
    val dims = Set(0)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.1), maxBirth = Some(2.3), minPersistence = Some(0.0), maxPersistence = Some(18.4)
        )
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 1)
    assert(result contains 0)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((2.3 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 2.3))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
  }

  it should "calculate correct pixel size when only some bounds are provided by config" in {
    val dims = Set(0)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.1), maxBirth = None, minPersistence = Some(0.0), maxPersistence = Some(18.4)
        )
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 1)
    assert(result contains 0)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((1.0 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 1.0))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
  }

  it should
    "calculate correct pixel size when all bounds are provided by config (multiple dims, each image square)" in {
    val dims = Set(0, 2)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixels = 100
    val dimensions = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.1), maxBirth = Some(2.3), minPersistence = Some(0.0), maxPersistence = Some(18.4)
        ),
      2 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(1.2), maxBirth = Some(454.2), minPersistence = Some(0.4), maxPersistence = Some(10.0)
        )
    )

    val result = PersistenceImage.calculatePixelSizesPerDim(rdd, numberOfPixels, numberOfPixels, dimensions)

    assert(result.size == 2)
    assert(result contains 0)
    assert(result contains 2)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((2.3 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 100.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 2.3))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
    assert(result(2)._1 == PersistenceImage.BirthAxisPixelSize((454.2 - 1.2) / 100.0))
    assert(result(2)._2 == PersistenceImage.PersistenceAxisPixelSize((10.0 - 0.4) / 100.0))
    assert(result(2)._3.xBound == SingleDimMinMaxBound[Double](1.2, 454.2))
    assert(result(2)._3.yBound == SingleDimMinMaxBound[Double](0.4, 10.0))
  }

  it should
    "calculate correct pixel size when all bounds are provided by config (multiple dims, each image rectangular)" in {
    val dims = Set(0, 2)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(0.1), maxBirth = Some(2.3), minPersistence = Some(0.0), maxPersistence = Some(18.4)
        ),
      2 ->
        BirthAndPersistenceBoundsConfig(
          minBirth = Some(1.2), maxBirth = Some(454.2), minPersistence = Some(0.4), maxPersistence = Some(10.0)
        )
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 2)
    assert(result contains 0)
    assert(result contains 2)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((2.3 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 2.3))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
    assert(result(2)._1 == PersistenceImage.BirthAxisPixelSize((454.2 - 1.2) / 100.0))
    assert(result(2)._2 == PersistenceImage.PersistenceAxisPixelSize((10.0 - 0.4) / 75.0))
    assert(result(2)._3.xBound == SingleDimMinMaxBound[Double](1.2, 454.2))
    assert(result(2)._3.yBound == SingleDimMinMaxBound[Double](0.4, 10.0))
  }


  it should "calculate correct pixel size when no bounds are provided by config (single dim, square image)" in {
    val dims = Set(0)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixels = 100
    val dimensions = Map(
      0 -> BirthAndPersistenceBoundsConfig()
    )

    val result = PersistenceImage.calculatePixelSizesPerDim(rdd, numberOfPixels, numberOfPixels, dimensions)

    assert(result.size == 1)
    assert(result contains 0)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((1.0 - 0.0) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 1.3) / 100.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.0, 1.0))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](1.3, 18.4))
  }

  it should "calculate correct pixel size when no bounds are provided by config (single dim, rectangular image)" in {
    val dims = Set(0)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 -> BirthAndPersistenceBoundsConfig()
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 1)
    assert(result contains 0)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((1.0 - 0.0) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 1.3) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.0, 1.0))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](1.3, 18.4))
  }

  it should
    "calculate correct pixel size when no bounds are provided by config (multiple dims, each image square)" in {
    val dims = Set(0, 2)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixels = 100
    val dimensions = Map(
      0 -> BirthAndPersistenceBoundsConfig(),
      2 -> BirthAndPersistenceBoundsConfig()
    )

    val result = PersistenceImage.calculatePixelSizesPerDim(rdd, numberOfPixels, numberOfPixels, dimensions)

    assert(result.size == 2)
    assert(result contains 0)
    assert(result contains 2)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((1.0 - 0.0) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 1.3) / 100.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.0, 1.0))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](1.3, 18.4))
    assert(result(2)._1 == PersistenceImage.BirthAxisPixelSize((454.2 - 1.2) / 100.0))
    assert(result(2)._2 == PersistenceImage.PersistenceAxisPixelSize((10.0 - 0.4) / 100.0))
    assert(result(2)._3.xBound == SingleDimMinMaxBound[Double](1.2, 454.2))
    assert(result(2)._3.yBound == SingleDimMinMaxBound[Double](0.4, 10.0))
  }

  it should
    "calculate correct pixel size when no bounds are provided by config (multiple dims, each image rectangular)" in {
    val dims = Set(0, 2)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 -> BirthAndPersistenceBoundsConfig(),
      2 -> BirthAndPersistenceBoundsConfig()
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 2)
    assert(result contains 0)
    assert(result contains 2)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((1.0 - 0.0) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 1.3) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.0, 1.0))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](1.3, 18.4))
    assert(result(2)._1 == PersistenceImage.BirthAxisPixelSize((454.2 - 1.2) / 100.0))
    assert(result(2)._2 == PersistenceImage.PersistenceAxisPixelSize((10.0 - 0.4) / 75.0))
    assert(result(2)._3.xBound == SingleDimMinMaxBound[Double](1.2, 454.2))
    assert(result(2)._3.yBound == SingleDimMinMaxBound[Double](0.4, 10.0))
  }

  it should
    "calculate correct pixel size when only bounds for one dim are provided by config " in {
    val dims = Set(0, 2)
    val rdd = birthPersistencePairsRDD(dims)
    val numberOfPixelsBirthAxis = 100
    val numberOfPixelsPersistenceAxis = 75
    val dimensions = Map(
      0 -> BirthAndPersistenceBoundsConfig(
        minBirth = Some(0.1), maxBirth = Some(2.3), minPersistence = Some(0.0), maxPersistence = Some(18.4)
      ),
      2 -> BirthAndPersistenceBoundsConfig()
    )

    val result = PersistenceImage
      .calculatePixelSizesPerDim(rdd, numberOfPixelsBirthAxis, numberOfPixelsPersistenceAxis, dimensions)

    assert(result.size == 2)
    assert(result contains 0)
    assert(result contains 2)
    assert(result(0)._1 == PersistenceImage.BirthAxisPixelSize((2.3 - 0.1) / 100.0))
    assert(result(0)._2 == PersistenceImage.PersistenceAxisPixelSize((18.4 - 0.0) / 75.0))
    assert(result(0)._3.xBound == SingleDimMinMaxBound[Double](0.1, 2.3))
    assert(result(0)._3.yBound == SingleDimMinMaxBound[Double](0.0, 18.4))
    assert(result(2)._1 == PersistenceImage.BirthAxisPixelSize((454.2 - 1.2) / 100.0))
    assert(result(2)._2 == PersistenceImage.PersistenceAxisPixelSize((10.0 - 0.4) / 75.0))
    assert(result(2)._3.xBound == SingleDimMinMaxBound[Double](1.2, 454.2))
    assert(result(2)._3.yBound == SingleDimMinMaxBound[Double](0.4, 10.0))
  }

}
