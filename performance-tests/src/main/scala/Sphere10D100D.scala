import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser

import scala.util.Random

object Sphere10D100D {

  def main(args: Array[String]): Unit = {
    val parser = Config.parser("Sphere10D100D")
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkSession.builder()
          .config(new SparkConf().setAppName(s"Performance Test - Sphere10D100D - ${config.caseName}"))
          .getOrCreate()

        val (numPoints, threshold) = config.caseName match {
          case "100k-threshold" => (100000, Some(0.8f))
          case _ => throw new IllegalArgumentException(s"Unknown case: ${config.caseName}")
        }

        val pointsCloud = generateSpherePoints(numPoints)

        config.pointsCloudOutputPath.foreach(path => Utils.savePointsCloud(pointsCloud, path))

        val persistencePairsArray = PersistentHomology.computePersistentHomology(
          pointsCloud,
          maxDim = 2,
          distanceThreshold = threshold
        )

        config.outputPath match {
          case Some(path) =>
            for (d <- 0 to 2) {
              Utils.savePersistencePairs(persistencePairsArray(d), s"$path/dim_$d")
            }
          case None =>
            for (d <- 0 to 2) {
              println(s"Dim $d pairs count: ${persistencePairsArray(d).count()}")
            }
        }

        spark.stop()
      case _ =>
    }
  }

  private def generateSpherePoints(numPoints: Int)(implicit spark: SparkSession): Dataset[Array[Float]] = {
    import spark.implicits._
    val random = new Random(42)
    val noiseScale = 0.01

    val points = (1 to numPoints).map { _ =>
      val rawCoords = Array.fill(11)(random.nextGaussian())
      val norm = Math.sqrt(rawCoords.map(x => x * x).sum)
      val sphereCoords = rawCoords.map(x => (x / norm).toFloat)

      val coords = Array.fill(100)((random.nextDouble() - 0.5).toFloat * noiseScale.toFloat)
      for (i <- 0 until 11) {
        coords(i) = coords(i) + sphereCoords(i)
      }

      coords
    }

    spark.createDataset(points)
  }

}

