package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import org.apache.spark.ml.linalg.DenseMatrix
import org.scalatest.flatspec.AnyFlatSpec
import org.scalactic.TolerantNumerics

class PersistenceImageMetricsSpec extends AnyFlatSpec {

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-5)

  val bound = PersistenceImage.ImageBound(0.0, 1.0)

  def createImage(values: Array[Double]): PersistenceImage = {
    PersistenceImage(
      new DenseMatrix(2, 2, values),
      bound,
      bound
    )
  }

  val imageA     = createImage(Array(1.0, 2.0, 3.0, 4.0))
  val imageB     = createImage(Array(4.0, 3.0, 2.0, 1.0))
  val imageZeros = createImage(Array(0.0, 0.0, 0.0, 0.0))

  behavior of "PersistenceImageMetrics.euclideanDistance"

  it should "correctly compute distance between two images" in {
    // A: [1, 2, 3, 4], B: [4, 3, 2, 1]
    // Diff: [-3, -1, 1, 3]
    // Sq: [9, 1, 1, 9] => sum = 20
    // sqrt(20) ~ 4.472135955
    val dist = PersistenceImageMetrics.euclideanDistance(imageA, imageB)
    assert(dist === math.sqrt(20.0))
  }

  it should "return 0 for identical images" in {
    val dist = PersistenceImageMetrics.euclideanDistance(imageA, imageA)
    assert(dist === 0.0)
  }

  behavior of "PersistenceImageMetrics.manhattanDistance"

  it should "correctly compute distance between two images" in {
    // A: [1, 2, 3, 4], B: [4, 3, 2, 1]
    // Diff abs: [3, 1, 1, 3] => sum = 8
    val dist = PersistenceImageMetrics.manhattanDistance(imageA, imageB)
    assert(dist === 8.0)
  }

  it should "return 0 for identical images" in {
    val dist = PersistenceImageMetrics.manhattanDistance(imageA, imageA)
    assert(dist === 0.0)
  }

  behavior of "PersistenceImageMetrics.chebyshevDistance"

  it should "correctly compute distance between two images" in {
    // A: [1, 2, 3, 4], B: [4, 3, 2, 1]
    // Diff abs: [3, 1, 1, 3] => max = 3
    val dist = PersistenceImageMetrics.chebyshevDistance(imageA, imageB)
    assert(dist === 3.0)
  }

  it should "return 0 for identical images" in {
    val dist = PersistenceImageMetrics.chebyshevDistance(imageA, imageA)
    assert(dist === 0.0)
  }

  behavior of "PersistenceImageMetrics.cosineSimilarity"

  it should "correctly compute similarity between two images" in {
    // A: [1, 2, 3, 4], B: [4, 3, 2, 1]
    // dot = 4 + 6 + 6 + 4 = 20
    // norm A = sqrt(1+4+9+16) = sqrt(30)
    // norm B = sqrt(16+9+4+1) = sqrt(30)
    // sim = 20 / 30 = 2/3
    val sim = PersistenceImageMetrics.cosineSimilarity(imageA, imageB)
    assert(sim === (2.0 / 3.0))
  }

  it should "return 1.0 for identical non-zero images" in {
    val sim = PersistenceImageMetrics.cosineSimilarity(imageA, imageA)
    assert(sim === 1.0)
  }

  it should "return 1.0 for two zero images" in {
    val sim = PersistenceImageMetrics.cosineSimilarity(imageZeros, imageZeros)
    assert(sim === 1.0)
  }

  it should "return 0.0 when one image is zero and other is not" in {
    val sim = PersistenceImageMetrics.cosineSimilarity(imageA, imageZeros)
    assert(sim === 0.0)
    val sim2 = PersistenceImageMetrics.cosineSimilarity(imageZeros, imageA)
    assert(sim2 === 0.0)
  }

  behavior of "PersistenceImageMetrics incomparability validation"

  it should "throw IllegalArgumentException when birth bounds differ" in {
    val imageDiffBirthBound = PersistenceImage(
      new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0)),
      PersistenceImage.ImageBound(0.0, 2.0),
      bound
    )
    assertThrows[IllegalArgumentException] {
      PersistenceImageMetrics.euclideanDistance(imageA, imageDiffBirthBound)
    }
  }

  it should "throw IllegalArgumentException when persistence bounds differ" in {
    val imageDiffPersBound = PersistenceImage(
      new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0)),
      bound,
      PersistenceImage.ImageBound(0.0, 2.0)
    )
    assertThrows[IllegalArgumentException] {
      PersistenceImageMetrics.euclideanDistance(imageA, imageDiffPersBound)
    }
  }

  it should "throw IllegalArgumentException when dimensions differ" in {
    val imageDiffDims = PersistenceImage(
      new DenseMatrix(1, 4, Array(1.0, 2.0, 3.0, 4.0)),
      bound,
      bound
    )
    assertThrows[IllegalArgumentException] {
      PersistenceImageMetrics.euclideanDistance(imageA, imageDiffDims)
    }
  }

}
