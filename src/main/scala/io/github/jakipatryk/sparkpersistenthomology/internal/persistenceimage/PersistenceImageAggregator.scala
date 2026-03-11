package io.github.jakipatryk.sparkpersistenthomology.internal.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.{
  InfluenceDistribution,
  WeightingFunction
}
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.IntegrationOverRectangle
import org.apache.spark.ml.linalg.{ DenseMatrix, Matrices }
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{ Encoder, Encoders }

private[sparkpersistenthomology] class PersistenceImageAggregator(
  numberOfPixelsOnBirthAxis: Int,
  numberOfPixelsOnPersistenceAxis: Int,
  birthAxisPixelSize: Double,
  persistenceAxisPixelSize: Double,
  minBirth: Double,
  minPersistence: Double,
  influenceDistribution: InfluenceDistribution,
  weightingFunction: WeightingFunction,
  monteCarloIntegrationSamplesPerPixel: Int
) extends Aggregator[PersistencePair, Array[Double], DenseMatrix] {

  override def zero: Array[Double] =
    Array.fill(numberOfPixelsOnBirthAxis * numberOfPixelsOnPersistenceAxis)(0.0)

  override def reduce(buffer: Array[Double], pair: PersistencePair): Array[Double] = {
    if (pair.isFinite) {
      val birth       = pair.birth.toDouble
      val persistence = pair.persistence.toDouble
      val weight      = weightingFunction.compute(birth, persistence)

      for {
        i <- 0 until numberOfPixelsOnBirthAxis
        j <- 0 until numberOfPixelsOnPersistenceAxis
      } {
        val pixelBottomLeftRelativeCoordinates = (
          minBirth + i * birthAxisPixelSize,
          minPersistence + j * persistenceAxisPixelSize
        )
        val integralOnlyInfluencedByThisPair = IntegrationOverRectangle.computeMonteCarloIntegral(
          (x, y) => influenceDistribution.compute((birth, persistence), (x, y)),
          pixelBottomLeftRelativeCoordinates,
          birthAxisPixelSize,
          persistenceAxisPixelSize,
          monteCarloIntegrationSamplesPerPixel
        )

        // Storing in column-major order to match DenseMatrix
        val index = j * numberOfPixelsOnBirthAxis + i
        buffer(index) += weight * integralOnlyInfluencedByThisPair
      }
    }
    buffer
  }

  override def merge(b1: Array[Double], b2: Array[Double]): Array[Double] = {
    var i   = 0
    val len = b1.length
    while (i < len) {
      b1(i) += b2(i)
      i += 1
    }
    b1
  }

  override def finish(reduction: Array[Double]): DenseMatrix = {
    new DenseMatrix(numberOfPixelsOnBirthAxis, numberOfPixelsOnPersistenceAxis, reduction)
  }

  override def bufferEncoder: Encoder[Array[Double]] = Encoders.kryo[Array[Double]]

  override def outputEncoder: Encoder[DenseMatrix] = Encoders.kryo[DenseMatrix]
}
