import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser

import scala.util.Random

object Torus2D {

  def main(args: Array[String]): Unit = {
    val parser = Config.parser("Torus2D")
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkSession.builder()
          .config(new SparkConf().setAppName(s"Performance Test - Torus2D - ${config.caseName}"))
          .getOrCreate()

        val (numPoints, threshold) = config.caseName match {
          case "4k-no-threshold" => (4000, None)
          case "100k-threshold" => (100000, Some(0.5f))
          case _ => throw new IllegalArgumentException(s"Unknown case: ${config.caseName}")
        }

        val pointsCloud = generateTorusPoints(numPoints)

        config.pointsCloudOutputPath.foreach(path => Utils.savePointsCloud(pointsCloud, path))

        val persistencePairsArray = PersistentHomology.computePersistentHomology(
          pointsCloud,
          maxDim = 1,
          distanceThreshold = threshold
        )

        config.outputPath match {
          case Some(path) =>
            for (d <- 0 to 1) {
              Utils.savePersistencePairs(persistencePairsArray(d), s"$path/dim_$d")
            }
          case None =>
            for (d <- 0 to 1) {
              println(s"Dim $d pairs count: ${persistencePairsArray(d).count()}")
            }
        }

        spark.stop()
      case _ =>
    }
  }

  private def generateTorusPoints(numPoints: Int)(implicit spark: SparkSession): Dataset[Array[Float]] = {
    import spark.implicits._
    val random = new Random(42)
    val R = 2.0
    val r = 1.0
    val noiseScale = 0.05

    val points = (1 to numPoints).map { _ =>
      val theta = random.nextDouble() * 2.0 * Math.PI
      val phi = random.nextDouble() * 2.0 * Math.PI
      val noiseX = (random.nextDouble() - 0.5) * noiseScale
      val noiseY = (random.nextDouble() - 0.5) * noiseScale
      val noiseZ = (random.nextDouble() - 0.5) * noiseScale

      Array(
        ((R + r * Math.cos(theta)) * Math.cos(phi) + noiseX).toFloat,
        ((R + r * Math.cos(theta)) * Math.sin(phi) + noiseY).toFloat,
        (r * Math.sin(theta) + noiseZ).toFloat
      )
    }

    spark.createDataset(points)
  }

}

