// Based on "Persistence Images: A Stable Vector Representation of Persistent Homology"
// (https://jmlr.org/papers/v18/16-337.html)

package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import io.github.jakipatryk.sparkpersistenthomology.internal.persistenceimage.PersistenceImageAggregator
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.util.{ Failure, Success, Try }

/** Represents configuration of bounds of an image. If any of them is None, its value is calculated
  * based of min/max values of birth/persistence (death - birth) of persistence pairs.
  */
case class BirthAndPersistenceBoundsConfig(
  minBirth: Option[Double] = None,
  maxBirth: Option[Double] = None,
  minPersistence: Option[Double] = None,
  maxPersistence: Option[Double] = None
)

case class PersistenceImage private (
  image: DenseMatrix,
  birthBound: PersistenceImage.ImageBound,
  persistenceBound: PersistenceImage.ImageBound
)

object PersistenceImage {

  final case class Birth(value: Double)       extends AnyVal
  final case class Persistence(value: Double) extends AnyVal
  final case class Weight(value: Double)      extends AnyVal

  final case class ImageBound(min: Double, max: Double)

  /** Generates persistence image from persistence pairs (output of persistence homology). The input
    * pairs should represent a single dimension.
    *
    * @param persistencePairs
    *   Output of `PersistentHomology.getPersistencePairs`, `Dataset` of `PersistencePair`
    *   representing birth, death, and dim of a hole (homology class). Should contain pairs of a
    *   single dimension.
    * @param boundsConfig
    *   `BirthAndPersistenceBoundsConfig` representing optional bounds of birth and persistence
    *   axis.
    * @param numberOfPixelsOnBirthAxis
    *   Resolution of image on birth axis.
    * @param numberOfPixelsOnPersistenceAxis
    *   Resolution of image on persistence axis.
    * @param influenceDistribution
    *   "Influence distribution" is used to calculate influence of a persistence pair on a given
    *   point (pixel) in the image. Typical one is gaussian distribution.
    * @param weightingFunction
    *   Function that weights overall influence of persistence point. Takes Birth and Persistence
    *   (death - birth) and outputs weight. Typical is to just take Persistence and ignore birth.
    * @param monteCarloIntegrationSamplesPerPixel
    *   Number of samples used in computing Monte Carlo Integral for each pixel and persistence
    *   pair. The higher number the better image approximation, but slower computation.
    * @return
    *   Persistence image with image in the form of DenseMatrix.
    */
  def fromPersistencePairs(
    persistencePairs: Dataset[PersistencePair],
    boundsConfig: BirthAndPersistenceBoundsConfig,
    numberOfPixelsOnBirthAxis: Int,
    numberOfPixelsOnPersistenceAxis: Int,
    influenceDistribution: InfluenceDistribution,
    weightingFunction: (Birth, Persistence) => Weight = (_, p) => Weight(p.value),
    monteCarloIntegrationSamplesPerPixel: Int = 5
  ): Try[PersistenceImage] = {
    import persistencePairs.sparkSession.implicits._

    val isConfigValid = validateDimensionsConfig(boundsConfig)
    isConfigValid.flatMap { _ =>
      val finitePairs = persistencePairs.filter(p => !p.death.isPosInfinity)

      val (
        calculatedMinBirth,
        calculatedMaxBirth,
        calculatedMinPersistence,
        calculatedMaxPersistence
      ) =
        if (
          boundsConfig.minBirth.isEmpty ||
          boundsConfig.maxBirth.isEmpty ||
          boundsConfig.minPersistence.isEmpty ||
          boundsConfig.maxPersistence.isEmpty
        ) {
          val row = finitePairs
            .map(p => (p.birth.toDouble, (p.death - p.birth).toDouble))
            .toDF("b", "p")
            .agg(min("b"), max("b"), min("p"), max("p"))
            .head()

          if (row.isNullAt(0)) {
            (0.0, 0.0, 0.0, 0.0)
          } else {
            (row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
          }
        } else {
          (0.0, 0.0, 0.0, 0.0)
        }

      val actualMinBirth       = boundsConfig.minBirth.getOrElse(calculatedMinBirth)
      val actualMaxBirth       = boundsConfig.maxBirth.getOrElse(calculatedMaxBirth)
      val actualMinPersistence = boundsConfig.minPersistence.getOrElse(calculatedMinPersistence)
      val actualMaxPersistence = boundsConfig.maxPersistence.getOrElse(calculatedMaxPersistence)

      val birthAxisPixelSize = (actualMaxBirth - actualMinBirth) / numberOfPixelsOnBirthAxis
      val persistenceAxisPixelSize =
        (actualMaxPersistence - actualMinPersistence) / numberOfPixelsOnPersistenceAxis

      if (birthAxisPixelSize <= 0) {
        Failure(
          new IllegalStateException(
            "Calculated pixel size on birth axis is incorrect. Please specify it by hand in config."
          )
        )
      } else if (persistenceAxisPixelSize <= 0) {
        Failure(
          new IllegalStateException(
            "Calculated pixel size on persistence axis is incorrect. Please specify it by hand in config."
          )
        )
      } else {
        val aggregator = new PersistenceImageAggregator(
          numberOfPixelsOnBirthAxis,
          numberOfPixelsOnPersistenceAxis,
          birthAxisPixelSize,
          persistenceAxisPixelSize,
          actualMinBirth,
          actualMinPersistence,
          influenceDistribution,
          (b, p) => weightingFunction(Birth(b), Persistence(p)).value,
          monteCarloIntegrationSamplesPerPixel
        )

        val imageMatrix = finitePairs.select(aggregator.toColumn).head()

        Success(
          PersistenceImage(
            imageMatrix,
            ImageBound(actualMinBirth, actualMaxBirth),
            ImageBound(actualMinPersistence, actualMaxPersistence)
          )
        )
      }
    }
  }

  /** Same as [[fromPersistencePairs]], but specifically for Gaussian influence distribution. Takes
    * variance on "birth axis" and on "persistence axis" as a parameter to generate influence
    * distribution.
    */
  def fromPersistencePairsGaussian(
    persistencePairs: Dataset[PersistencePair],
    boundsConfig: BirthAndPersistenceBoundsConfig,
    numberOfPixelsOnBirthAxis: Int,
    numberOfPixelsOnPersistenceAxis: Int,
    varianceBirthAxis: Double,
    variancePersistenceAxis: Double
  ): Try[PersistenceImage] =
    fromPersistencePairs(
      persistencePairs,
      boundsConfig,
      numberOfPixelsOnBirthAxis,
      numberOfPixelsOnPersistenceAxis,
      new InfluenceDistribution.GaussianInfluenceDistribution(
        varianceBirthAxis,
        variancePersistenceAxis
      )
    )

  /** Same as [[fromPersistencePairs]], but specifically for Gaussian influence distribution. Takes
    * variance as a parameter to generate influence distribution.
    */
  def fromPersistencePairsGaussian(
    persistencePairs: Dataset[PersistencePair],
    boundsConfig: BirthAndPersistenceBoundsConfig,
    numberOfPixelsOnBirthAxis: Int,
    numberOfPixelsOnPersistenceAxis: Int,
    variance: Double
  ): Try[PersistenceImage] =
    fromPersistencePairsGaussian(
      persistencePairs,
      boundsConfig,
      numberOfPixelsOnBirthAxis,
      numberOfPixelsOnPersistenceAxis,
      variance,
      variance
    )

  private def validateDimensionsConfig(
    config: BirthAndPersistenceBoundsConfig
  ): Try[Unit] = {
    val areBirthBoundsInvalid = for {
      minBirth <- config.minBirth
      maxBirth <- config.maxBirth
    } yield minBirth >= maxBirth
    val arePersistenceBoundsInvalid = for {
      minPersistence <- config.minPersistence
      maxPersistence <- config.maxPersistence
    } yield minPersistence >= maxPersistence

    val errors = (if (areBirthBoundsInvalid.getOrElse(false))
                    Seq("Birth bounds in config are invalid.")
                  else Seq.empty[String]) ++
      (if (arePersistenceBoundsInvalid.getOrElse(false))
         Seq("Persistence bounds in config are invalid.")
       else Seq.empty[String])

    if (errors.isEmpty) Success(())
    else Failure(new IllegalArgumentException(errors.mkString(" ")))
  }

}
