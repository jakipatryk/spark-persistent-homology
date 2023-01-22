import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology
import io.github.jakipatryk.sparkpersistenthomology.filtrations.PointsCloud
import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.{BirthAndPersistenceBoundsConfig, PersistenceImage}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Random, Success}

case class Config(
                   numberOfPoints: Int = 50,
                   dim: Int = 8,
                   maxHomologyDim: Int = 3,
                   computePersistenceImage: Boolean = false,
                   numberOfPartitions: Option[Int] = None
                 )

object Config {
  import scopt.OParser

  private val builder = OParser.builder[Config]

  private val parser = {
    import builder._

    OParser.sequence(
      programName("random-points-cloud"),
      opt[Int]('n', "numberOfPoints")
        .action((n, c) => c.copy(numberOfPoints = n))
        .text("number of points in the generated random points cloud"),
      opt[Int]('d', "dim")
        .action((d, c) => c.copy(dim = d))
        .text("dimension of points in the generated random points cloud"),
      opt[Int]("maxHomologyDim")
        .action((maxDim, c) => c.copy(maxHomologyDim = maxDim))
        .text("max dimension of homology classes"),
      opt[Boolean] ("computePersistenceImage")
        .action((computePersistenceImage, c) => c.copy(computePersistenceImage = computePersistenceImage))
        .text("should the last step of the job be persistence image or just persistence pairs"),
      opt[Int] ("numberOfPartitions")
        .action((numberOfPartitions, c) => c.copy(numberOfPartitions = Some(numberOfPartitions)))
        .text("number of partitions used to compute persistence pairs")
    )
  }

  def getConfig(cliArgs: Seq[String]): Option[Config] = OParser.parse(parser, cliArgs, Config())

}

object SparkJob {

  private val sparkContext: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName(
        "Example of io.github.jakipatryk.spark-persistent-homology usage " +
          "- random points cloud, computing persistence pairs (and optionally persistence image)"
      )
  )

  def main(args: Array[String]): Unit = {
    val parsedConfig = Config.getConfig(args)

    parsedConfig match {
      case Some(Config(numberOfPoints, dim, maxHomologyDim, computePersistenceImage, numberOfPartitions)) =>
        val pointsCloud = generatePointsCloud(dim, numberOfPoints)

        val persistencePairs = PersistentHomology.getPersistencePairs(
          pointsCloud,
          numOfPartitionsConf = numberOfPartitions,
          maxDim = Some(maxHomologyDim)
        )

        if(computePersistenceImage) {
          // not including dim 0
          val dimensionsToInclude = (1 to maxHomologyDim).map(_ -> BirthAndPersistenceBoundsConfig()).toMap
          val numberOfPixelsOnBirthAxisPerDim = 100
          val numberOfPixelsOnPersistenceAxis = 100
          val variance = 1.0
          val computedImage = PersistenceImage.fromPersistencePairsGaussian(
            persistencePairs,
            dimensionsToInclude,
            numberOfPixelsOnBirthAxisPerDim,
            numberOfPixelsOnPersistenceAxis,
            variance
          )
          computedImage match {
            case Success(img) =>
              img.image.foreach(v => println(v.mkString(",")))
            case Failure(exception) =>
              println(s"Failed to generate persistence image, reason: ${exception.getMessage}")
          }
        } else {
          val numberOfPersistencePairs = persistencePairs.count()
          println(s"Number of persistence pairs is $numberOfPersistencePairs")
        }
    }

    sparkContext.stop()
  }

  private def generatePointsCloud(dim: Int, numberOfPoints: Int): PointsCloud = {
    val scalingFactor = 1000.0
    val randomPoints = Seq.fill(numberOfPoints)(Vector.fill(dim)(scalingFactor * Random.nextDouble))
    val rdd = sparkContext.parallelize(randomPoints)
    PointsCloud(rdd)
  }

}
