// Based on "Persistence Images: A Stable Vector Representation of Persistent Homology"
// (https://jmlr.org/papers/v18/16-337.html)

package com.jakipatryk.spark.persistenthomology.persistenceimage

import com.jakipatryk.spark.persistenthomology.PersistencePair
import com.jakipatryk.spark.persistenthomology.persistenceimage.InfluenceDistribution._
import com.jakipatryk.spark.persistenthomology.utils.{IntegrationOverRectangle, MinMaxBounds2D, SingleDimMinMaxBound}
import org.apache.spark.rdd.RDD

/**
 * Represents bounds of an image for a given dim.
 * If any of them is None, its value is calculated based of min/max values
 * of birth/persistence (death - birth) of persistence pairs.
 */
case class BirthAndPersistenceBoundsConfig(
                                            minBirth: Option[Double] = None,
                                            maxBirth: Option[Double] = None,
                                            minPersistence: Option[Double] = None,
                                            maxPersistence: Option[Double] = None
                                          )

case class PersistenceImage private(
                                     image: Vector[Vector[Double]],
                                     private val includedDimensions: Map[Int, MinMaxBounds2D[Double]]
                                   ) {

  def dims: Set[Int] = includedDimensions.keySet

  def minBirthInDim(dim: Int): Double = includedDimensions(dim).xBound.min
  def maxBirthInDim(dim: Int): Double = includedDimensions(dim).xBound.max
  def birthBoundInDim(dim: Int): SingleDimMinMaxBound[Double] = includedDimensions(dim).xBound

  def minPersistenceInDim(dim: Int): Double = includedDimensions(dim).yBound.min
  def maxPersistenceInDim(dim: Int): Double = includedDimensions(dim).yBound.max
  def persistenceBoundInDim(dim: Int): SingleDimMinMaxBound[Double] = includedDimensions(dim).yBound

}

object PersistenceImage {

  final case class Birth(value: Double) extends AnyVal
  final case class Persistence(value: Double) extends AnyVal
  final case class Weight(value: Double) extends AnyVal

  private[persistenceimage] case class BirthAxisPixelSize(value: Double) extends AnyVal

  private[persistenceimage] case class PersistenceAxisPixelSize(value: Double) extends AnyVal

  /**
   * Generates persistence image from persistence pairs (output of persistence homology).
   * If multiple dimensions are requested (in `dimensionsToInclude`),
   * their corresponding images are concatenated side by side on birth axis,
   * in natural order of dimensions (dim x before dim y iif x < y).
   *
   * @param persistencePairs Output of `PersistentHomology.getPersistencePairs`,
   *                         `RDD` of `PersistencePair` representing birth, death, and dim of a hole (homology class).
   * @param dimensionsToInclude `Map` where keys are dimensions to include in the image,
   *                            and values are `BirthAndPersistenceBoundsConfig` representing
   *                            optional bounds of birth and persistence axis
   *                            in part of the image corresponding to that dim.
   * @param numberOfPixelsOnBirthAxisPerDim Resolution of image on birth axis (per each dim).
   * @param numberOfPixelsOnPersistenceAxis Resolution of image on persistence axis.
   * @param influenceDistribution "Influence distribution" is used to calculate influence of a persistence pair
   *                              on a given point (pixel) in the image. Typical one is gaussian distribution.
   * @param weightingFunction Function that weights overall influence of persistence point.
   *                          Takes Birth and Persistence (death - birth) and outputs weight.
   *                          Typical is to just take Persistence and ignore birth.
   * @param monteCarloIntegrationSamplesPerPixel Number of samples used in computing Monte Carlo Integral
   *                                             for each pixel and persistence pair.
   *                                             The higher number the better image approximation,
   *                                             but slower computation.
   * @return Persistence image in the form of Vector of Vectors of Doubles.
   *         First coordinate is "birth axis", second "persistence axis".
   */
  def fromPersistencePairs(
                            persistencePairs: RDD[PersistencePair],
                            dimensionsToInclude: Map[Int, BirthAndPersistenceBoundsConfig],
                            numberOfPixelsOnBirthAxisPerDim: Int,
                            numberOfPixelsOnPersistenceAxis: Int,
                            influenceDistribution: InfluenceDistribution,
                            weightingFunction: (Birth, Persistence) => Weight = (_, p) => Weight(p.value),
                            monteCarloIntegrationSamplesPerPixel: Int = 5
                          ): PersistenceImage = {
    val finitePersistencePairs = persistencePairs.filter(_.death.isLeft)
    val onlyNeededDims = finitePersistencePairs.filter(dimensionsToInclude contains _.dim)
    val birthPersistencePairs = onlyNeededDims
      .map(p => (p.dim, (Birth(p.birth), Persistence(p.death.left.get - p.birth))))

    val pixelSizesAndBoundsPerDim = calculatePixelSizesPerDim(
      birthPersistencePairs, numberOfPixelsOnBirthAxisPerDim, numberOfPixelsOnPersistenceAxis, dimensionsToInclude
    )

    val dimsSorted = dimensionsToInclude.keys.toVector.sorted.zipWithIndex.toMap

    val initialImage = Array.fill(numberOfPixelsOnBirthAxisPerDim * dimensionsToInclude.size)(
        Array.fill(numberOfPixelsOnPersistenceAxis)(0.0D)
    )

    val image = birthPersistencePairs.aggregate(initialImage)(
        { case (currentImage, (dim, (birth, persistence))) =>
          val (birthAxisPixelSize, persistenceAxisPixelSize, _) =  pixelSizesAndBoundsPerDim(dim)
          val dimIndex = dimsSorted(dim)
          for(
            i <- dimIndex * numberOfPixelsOnBirthAxisPerDim until (dimIndex + 1) * numberOfPixelsOnBirthAxisPerDim;
            j <- 0 until numberOfPixelsOnPersistenceAxis
          ) {
            val pixelBottomLeftRelativeCoordinates = (
              (i - dimIndex * numberOfPixelsOnBirthAxisPerDim) * birthAxisPixelSize.value,
              j * persistenceAxisPixelSize.value
            )
            val integralOnlyInfluencedByThisPair = IntegrationOverRectangle.computeMonteCarloIntegral(
              (x, y) => influenceDistribution.compute((birth.value, persistence.value), (x, y)),
              pixelBottomLeftRelativeCoordinates,
              birthAxisPixelSize.value,
              persistenceAxisPixelSize.value,
              monteCarloIntegrationSamplesPerPixel
            )
            currentImage(i)(j) += weightingFunction(birth, persistence).value * integralOnlyInfluencedByThisPair
          }
          currentImage
        },
        { case (partialImage1, partialImage2) =>
          for(
            i <- 0 until numberOfPixelsOnBirthAxisPerDim * dimsSorted.size;
            j <- 0 until numberOfPixelsOnPersistenceAxis
          )
            partialImage1(i)(j) += partialImage2(i)(j)
          partialImage1
        }
      )

    val imageAsVector = image.toVector.map(_.toVector)
    PersistenceImage(imageAsVector, pixelSizesAndBoundsPerDim.mapValues { case (_, _, bounds) => bounds })
  }

  /**
   * Same as [[fromPersistencePairs]], but specifically for Gaussian influence distribution.
   * Takes variance on "birth axis" and on "persistence axis" as a parameter to generate influence distribution.
   */
  def fromPersistencePairsGaussian(
                                    persistencePairs: RDD[PersistencePair],
                                    dimensionsToInclude: Map[Int, BirthAndPersistenceBoundsConfig],
                                    numberOfPixelsOnBirthAxisPerDim: Int,
                                    numberOfPixelsOnPersistenceAxis: Int,
                                    varianceBirthAxis: Double,
                                    variancePersistenceAxis: Double
                                  ): PersistenceImage =
    fromPersistencePairs(
      persistencePairs,
      dimensionsToInclude,
      numberOfPixelsOnBirthAxisPerDim,
      numberOfPixelsOnPersistenceAxis,
      new GaussianInfluenceDistribution(varianceBirthAxis, variancePersistenceAxis)
    )

  /**
   * Same as [[fromPersistencePairs]], but specifically for Gaussian influence distribution.
   * Takes variance as a parameter to generate influence distribution.
   */
  def fromPersistencePairsGaussian(
                                    persistencePairs: RDD[PersistencePair],
                                    dimensionsToInclude: Map[Int, BirthAndPersistenceBoundsConfig],
                                    numberOfPixelsOnBirthAxisPerDim: Int,
                                    numberOfPixelsOnPersistenceAxis: Int,
                                    variance: Double
                                  ): PersistenceImage =
    fromPersistencePairsGaussian(
      persistencePairs,
      dimensionsToInclude,
      numberOfPixelsOnBirthAxisPerDim,
      numberOfPixelsOnPersistenceAxis,
      variance,
      variance
    )

  private[persistenceimage] def calculatePixelSizesPerDim(birthPersistencePairs: RDD[(Int, (Birth, Persistence))],
                                                          numberOfPixelsOnBirthAxisPerDim: Int,
                                                          numberOfPixelsOnPersistenceAxis: Int,
                                                          dimensions: Map[Int, BirthAndPersistenceBoundsConfig])
  : Map[Int, (BirthAxisPixelSize, PersistenceAxisPixelSize, MinMaxBounds2D[Double])] = {

    lazy val boundsPerDimCalculated = MinMaxBounds2D.boundsPerKeyFromRDD[Int, Double](
      birthPersistencePairs.map {
        case (dim, (b, p)) => (dim, (b.value, p.value))
      }
    )

    val isEveryConfigurationProvided = dimensions
      .forall(
        _._2
          .productIterator
          .forall { case conf: Option[Double] => conf.isDefined }
      )

    val boundsPerDim =
      if(isEveryConfigurationProvided)
        dimensions.map {
          case (dim, conf) =>
            (dim, MinMaxBounds2D(
              SingleDimMinMaxBound[Double](conf.minBirth.get, conf.maxBirth.get),
              SingleDimMinMaxBound[Double](conf.minPersistence.get, conf.maxPersistence.get)
            ))
        }
      else
        boundsPerDimCalculated.map {
          case (dim, bounds) =>
            val birthMinPerConfig = dimensions.get(dim).flatMap(_.minBirth)
            val birthMaxPerConfig = dimensions.get(dim).flatMap(_.maxBirth)
            val persistenceMinPerConfig = dimensions.get(dim).flatMap(_.minPersistence)
            val persistenceMaxPerConfig = dimensions.get(dim).flatMap(_.maxPersistence)

            val birthBound = SingleDimMinMaxBound[Double](
              birthMinPerConfig.getOrElse(bounds.xBound.min),
              birthMaxPerConfig.getOrElse(bounds.xBound.max)
            )
            val persistenceBound = SingleDimMinMaxBound[Double](
              persistenceMinPerConfig.getOrElse(bounds.yBound.min),
              persistenceMaxPerConfig.getOrElse(bounds.yBound.max)
            )

            (dim, MinMaxBounds2D(birthBound, persistenceBound))
        }

    boundsPerDim.map {
      case (dim, bounds) =>
        (
          dim,
          (
            BirthAxisPixelSize((bounds.xBound.max - bounds.xBound.min) / numberOfPixelsOnBirthAxisPerDim),
            PersistenceAxisPixelSize((bounds.yBound.max - bounds.yBound.min) / numberOfPixelsOnPersistenceAxis),
            bounds
          )
        )
    }
  }

}
