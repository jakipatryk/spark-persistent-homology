package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.PersistenceImage.{
  Birth,
  Persistence
}
import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.scalactic.{ Equality, TolerantNumerics }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.Source

class PersistenceImageSpec extends AnyFlatSpec with BeforeAndAfterAll {

  implicit var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("PersistenceImageSpec")
      .master("local[*]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  behavior of "fromPersistencePairs"

  val birthDeathPairs: List[PersistencePair] = List(
    PersistencePair(0, 0.0f, 10.0f),
    PersistencePair(0, 1.0f, 2.3f),
    PersistencePair(0, 0.2f, 18.6f),
    PersistencePair(0, 0.0f, 6.2f),
    PersistencePair(0, 8.0f, 15.0f),
    PersistencePair(0, 0.0f, PersistencePair.Infinity)
  )

  def birthDeathPairsDataset: Dataset[PersistencePair] = {
    val spark = sparkSession
    import spark.implicits._
    spark.createDataset(birthDeathPairs)
  }

  def imagesAlmostEqual(
    image1: DenseMatrix,
    image2: DenseMatrix,
    tolerance: Double = 1e-3f
  ): Boolean = {
    implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(tolerance)
    if (image1.numRows != image2.numRows || image1.numCols != image2.numCols) return false

    var equal = true
    for (i <- 0 until image1.numRows; j <- 0 until image1.numCols) {
      if (image1(i, j) !== image2(i, j)) {
        equal = false
      }
    }
    equal
  }

  def loadImageMatrixFromFile(path: String): DenseMatrix = {
    val source = Source
      .fromURL(getClass.getResource(path))
    val rows = source.mkString
      .split("\n")
      .filter(_.nonEmpty)
      .map(s => s.split(",").map(_.toDouble))

    val numRows = rows.length
    val numCols = rows(0).length
    val values  = new Array[Double](numRows * numCols)

    // CSV is typically row-major, DenseMatrix is column-major
    for (i <- 0 until numRows; j <- 0 until numCols) {
      values(j * numRows + i) = rows(i)(j)
    }

    source.close()
    new DenseMatrix(numRows, numCols, values)
  }

  it should
    "output persistence image close in values to ones calculated by scikit-tda with the same settings (single dim)" in {
      val boundsConfig = BirthAndPersistenceBoundsConfig(
        minBirth = Some(0.0),
        maxBirth = Some(12.1),
        minPersistence = Some(0.0),
        maxPersistence = Some(12.1)
      )

      val result = PersistenceImage.fromPersistencePairsGaussian(
        birthDeathPairsDataset,
        boundsConfig,
        100,
        100,
        1.0
      )

      val fromScikitTDA =
        loadImageMatrixFromFile("/persistenceimage/image-single-dim-scikit-tda.csv")

      assert(result.image.numRows === 100)
      assert(result.image.numCols === 100)
      assert(imagesAlmostEqual(fromScikitTDA, result.image))
    }

  it should "throw IllegalStateException when birth range of pairs is 0 and birth bounds are not specified by config" in {
    val singlePointPairs = Seq(
      PersistencePair(0, 0.0f, 10.0f),
      PersistencePair(0, 0.0f, 18.6f),
      PersistencePair(0, 0.0f, 6.2f),
      PersistencePair(0, 0.0f, 15.0f),
      PersistencePair(0, 0.0f, PersistencePair.Infinity)
    )
    val spark = sparkSession
    import spark.implicits._
    val singlePointDataset = spark.createDataset(singlePointPairs)
    val boundsConfig       = BirthAndPersistenceBoundsConfig()

    assertThrows[IllegalStateException] {
      PersistenceImage.fromPersistencePairsGaussian(
        singlePointDataset,
        boundsConfig,
        100,
        100,
        1.0
      )
    }
  }

  it should "throw IllegalArgumentException when bounds in config are invalid" in {
    val spark = sparkSession
    import spark.implicits._
    val ds = spark.createDataset(birthDeathPairs)
    val boundsConfig = BirthAndPersistenceBoundsConfig(
      minBirth = Some(10.0),
      maxBirth = Some(0.0)
    )

    assertThrows[IllegalArgumentException] {
      PersistenceImage.fromPersistencePairsGaussian(
        ds,
        boundsConfig,
        100,
        100,
        1.0
      )
    }
  }

}
