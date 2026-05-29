import io.github.jakipatryk.sparkpersistenthomology.{FiltrationConfig, PersistencePair, PersistentHomology}
import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.{BirthAndPersistenceBoundsConfig, PersistenceImage}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

case class Config(
                   numberOfPoints: Int = 50,
                   dim: Int = 8,
                   maxHomologyDim: Int = 3,
                   k: Option[Int] = None,
                   computePersistenceImage: Boolean = false
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
        .text("max dimension of homology to compute"),
      opt[Int]('k', "k")
        .action((kVal, c) => c.copy(k = Some(kVal)))
        .text("number of nearest neighbors for mutual k-NN filtration. If not specified, Vietoris-Rips filtration is used."),
      opt[Boolean] ("computePersistenceImage")
        .action((computePersistenceImage, c) => c.copy(computePersistenceImage = computePersistenceImage))
        .text("should the last step of the job be persistence image or just persistence pairs")
    )
  }

  def getConfig(cliArgs: Seq[String]): Option[Config] = OParser.parse(parser, cliArgs, Config())

}

object SparkJob {

  private implicit val sparkSession: SparkSession = {
    SparkSession.builder()
      .config(
        new SparkConf()
          .setAppName(
            "Example of io.github.jakipatryk.spark-persistent-homology usage " +
              "- random points cloud, computing persistence pairs (and optionally persistence image)"
          )
      )
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val parsedConfig = Config.getConfig(args)

    parsedConfig match {
      case Some(Config(numberOfPoints, dim, maxHomologyDim, k, computePersistenceImage)) =>
        val pointsCloud = generatePointsCloud(dim, numberOfPoints)

        val filtrationConfig = k match {
          case Some(kVal) => FiltrationConfig.NearestNeighbors(k = kVal)
          case None => FiltrationConfig.VietorisRips()
        }

        val persistencePairsArray = PersistentHomology.computePersistentHomology(
          pointsCloud,
          maxDim = maxHomologyDim,
          filtrationConfig = filtrationConfig
        )

        if(computePersistenceImage) {
          // not including dim 0
          val numberOfPixelsOnBirthAxis = 100
          val numberOfPixelsOnPersistenceAxis = 100
          val variance = 1.0

          for (d <- 1 to maxHomologyDim) {
            println(s"Persistence image for dimension $d:")
            val pairsForDim = persistencePairsArray(d)
            try {
              val computedImage = PersistenceImage.fromPersistencePairsGaussian(
                pairsForDim,
                BirthAndPersistenceBoundsConfig(),
                numberOfPixelsOnBirthAxis,
                numberOfPixelsOnPersistenceAxis,
                variance
              )

              val img = computedImage.image
              for (i <- 0 until img.numRows) {
                val row = for (j <- 0 until img.numCols) yield img(i, j)
                println(row.mkString(","))
              }
            } catch {
              case exception: Exception =>
                println(s"Failed to generate persistence image for dimension $d, reason: ${exception.getMessage}")
            }
          }
        } else {
          for (d <- 0 to maxHomologyDim) {
            val numberOfPersistencePairs = persistencePairsArray(d).count()
            println(s"Number of persistence pairs for dimension $d is $numberOfPersistencePairs")
          }
        }
      case None =>
    }

    sparkSession.stop()
  }

  private def generatePointsCloud(dim: Int, numberOfPoints: Int): Dataset[Array[Float]] = {
    import sparkSession.implicits._
    val scalingFactor = 1000.0f
    val randomPoints = Seq.fill(numberOfPoints)(Array.fill(dim)(scalingFactor * Random.nextFloat()))
    sparkSession.createDataset(randomPoints)
  }

}
