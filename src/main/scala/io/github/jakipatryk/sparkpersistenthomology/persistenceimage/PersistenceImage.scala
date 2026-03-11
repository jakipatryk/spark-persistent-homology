// Based on "Persistence Images: A Stable Vector Representation of Persistent Homology"
// (https://jmlr.org/papers/v18/16-337.html)

package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import io.github.jakipatryk.sparkpersistenthomology.internal.persistenceimage.PersistenceImageAggregator
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class PersistenceImage private (
  image: DenseMatrix,
  birthBound: PersistenceImage.ImageBound,
  persistenceBound: PersistenceImage.ImageBound
)

object PersistenceImage {

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
    *   "Weighting function" that weights overall influence of persistence point. Takes birth and
    *   persistence (death - birth) and outputs weight. Typical is to just take persistence and
    *   ignore birth.
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
    weightingFunction: WeightingFunction = WeightingFunction.JustPersistenceWeightingFunction,
    monteCarloIntegrationSamplesPerPixel: Int = 5
  ): PersistenceImage = {
    validateDimensionsConfig(boundsConfig)

    val finitePairs = persistencePairs.filter(p => !p.death.isPosInfinity)

    val (birthBound, persistenceBound) = resolveBounds(finitePairs, boundsConfig)

    val birthAxisPixelSize = (birthBound.max - birthBound.min) / numberOfPixelsOnBirthAxis
    val persistenceAxisPixelSize =
      (persistenceBound.max - persistenceBound.min) / numberOfPixelsOnPersistenceAxis

    if (birthAxisPixelSize <= 0) {
      throw new IllegalStateException(
        "Calculated pixel size on birth axis is incorrect. Please specify it by hand in config."
      )
    } else if (persistenceAxisPixelSize <= 0) {
      throw new IllegalStateException(
        "Calculated pixel size on persistence axis is incorrect. Please specify it by hand in config."
      )
    } else {
      val aggregator = new PersistenceImageAggregator(
        numberOfPixelsOnBirthAxis,
        numberOfPixelsOnPersistenceAxis,
        birthAxisPixelSize,
        persistenceAxisPixelSize,
        birthBound.min,
        persistenceBound.min,
        influenceDistribution,
        weightingFunction,
        monteCarloIntegrationSamplesPerPixel
      )

      val imageMatrix = finitePairs.select(aggregator.toColumn).head()

      PersistenceImage(
        imageMatrix,
        birthBound,
        persistenceBound
      )
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
  ): PersistenceImage =
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
  ): PersistenceImage =
    fromPersistencePairsGaussian(
      persistencePairs,
      boundsConfig,
      numberOfPixelsOnBirthAxis,
      numberOfPixelsOnPersistenceAxis,
      variance,
      variance
    )

  private def resolveBounds(
    finitePairs: Dataset[PersistencePair],
    boundsConfig: BirthAndPersistenceBoundsConfig
  ): (ImageBound, ImageBound) = {
    import finitePairs.sparkSession.implicits._

    val (calcMinB, calcMaxB, calcMinP, calcMaxP) =
      if (
        boundsConfig.minBirth.isEmpty ||
        boundsConfig.maxBirth.isEmpty ||
        boundsConfig.minPersistence.isEmpty ||
        boundsConfig.maxPersistence.isEmpty
      ) {
        val row = finitePairs
          .map(p => (p.birth.toDouble, p.persistence.toDouble))
          .agg(min("_1"), max("_1"), min("_2"), max("_2"))
          .head()

        if (row.isNullAt(0)) (0.0, 0.0, 0.0, 0.0)
        else (row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
      } else {
        (0.0, 0.0, 0.0, 0.0)
      }

    (
      ImageBound(
        boundsConfig.minBirth.getOrElse(calcMinB),
        boundsConfig.maxBirth.getOrElse(calcMaxB)
      ),
      ImageBound(
        boundsConfig.minPersistence.getOrElse(calcMinP),
        boundsConfig.maxPersistence.getOrElse(calcMaxP)
      )
    )
  }

  private def validateDimensionsConfig(
    config: BirthAndPersistenceBoundsConfig
  ): Unit = {
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

    if (errors.nonEmpty) {
      throw new IllegalArgumentException(errors.mkString(" "))
    }
  }

}
