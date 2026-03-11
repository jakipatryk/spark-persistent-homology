package io.github.jakipatryk.sparkpersistenthomology.internal.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.{
  InfluenceDistribution,
  WeightingFunction
}
import org.apache.spark.ml.linalg.DenseMatrix
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PersistenceImageAggregatorSpec extends AnyFlatSpec with Matchers {

  class ConstantInfluenceDistribution(value: Double) extends InfluenceDistribution {
    override def compute(center: (Double, Double), point: (Double, Double)): Double = value
  }

  class ConstantWeightingFunction(value: Double) extends WeightingFunction {
    override def compute(birth: Double, persistence: Double): Double = value
  }

  val numPixelsBirth       = 2
  val numPixelsPersistence = 2
  val birthSize            = 1.0
  val persistenceSize      = 1.0
  val minBirth             = 0.0
  val minPersistence       = 0.0
  val influence            = new ConstantInfluenceDistribution(1.0)
  val weighting            = new ConstantWeightingFunction(1.0)
  val samples              = 1

  val aggregator = new PersistenceImageAggregator(
    numPixelsBirth,
    numPixelsPersistence,
    birthSize,
    persistenceSize,
    minBirth,
    minPersistence,
    influence,
    weighting,
    samples
  )

  "zero" should "return an array of zeros of correct size" in {
    val z = aggregator.zero
    z should have size (numPixelsBirth * numPixelsPersistence)
    z.forall(_ == 0.0) shouldBe true
  }

  "reduce" should "correctly update buffer for a finite pair" in {
    val buffer = aggregator.zero
    val pair   = PersistencePair(0, 0.5f, 1.5f) // persistence = 1.0

    val result = aggregator.reduce(buffer, pair)

    // Each pixel is 1x1. Influence is 1.0. Weight is 1.0.
    // Integration over 1x1 rectangle of constant 1.0 is 1.0.
    // So each pixel should get 1.0 * 1.0 = 1.0.
    result should have size 4
    result.forall(_ == 1.0) shouldBe true
  }

  it should "do nothing for an infinite pair" in {
    val buffer = aggregator.zero
    val pair   = PersistencePair(0, 0.5f, Float.PositiveInfinity)

    val result = aggregator.reduce(buffer, pair)

    result.forall(_ == 0.0) shouldBe true
  }

  "merge" should "correctly combine two buffers" in {
    val b1 = Array(1.0, 2.0, 3.0, 4.0)
    val b2 = Array(0.5, 0.5, 0.5, 0.5)

    val result = aggregator.merge(b1, b2)

    result shouldBe Array(1.5, 2.5, 3.5, 4.5)
  }

  "finish" should "correctly convert buffer to DenseMatrix" in {
    // buffer is in column-major order for DenseMatrix
    // numberOfPixelsOnBirthAxis = 2
    // numberOfPixelsOnPersistenceAxis = 2
    // Matrix [ [1.0, 3.0], [2.0, 4.0] ]
    // (first col is 1.0, 2.0; second col is 3.0, 4.0)
    val buffer = Array(1.0, 2.0, 3.0, 4.0)

    val matrix = aggregator.finish(buffer)

    matrix.numRows shouldBe 2
    matrix.numCols shouldBe 2
    matrix(0, 0) shouldBe 1.0
    matrix(1, 0) shouldBe 2.0
    matrix(0, 1) shouldBe 3.0
    matrix(1, 1) shouldBe 4.0
  }

}
