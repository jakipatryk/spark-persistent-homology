import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser

import scala.util.Random

object Torus3D100D {

  def main(args: Array[String]): Unit = {
    val parser = Config.parser("Torus3D100D")
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkSession.builder()
          .config(new SparkConf().setAppName(s"Performance Test - Torus3D100D - ${config.caseName}"))
          .getOrCreate()

        val (numPoints, threshold) = config.caseName match {
          case "no-threshold" => (1000, None)
          case "100k-threshold" => (100000, Some(0.3f))
          case _ => throw new IllegalArgumentException(s"Unknown case: ${config.caseName}")
        }

        val pointsCloud = generateTorus3DPoints(numPoints)

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

  private def generateTorus3DPoints(numPoints: Int)(implicit spark: SparkSession): Dataset[Array[Float]] = {
    import spark.implicits._
    val random = new Random(42)
    val noiseScale = 0.01

    val points = (1 to numPoints).map { _ =>
      val theta1 = random.nextDouble() * 2.0 * Math.PI
      val theta2 = random.nextDouble() * 2.0 * Math.PI
      val theta3 = random.nextDouble() * 2.0 * Math.PI

      val coords = Array.fill(100)((random.nextDouble() - 0.5).toFloat * noiseScale.toFloat)
      coords(0) = (coords(0) + Math.cos(theta1)).toFloat
      coords(1) = (coords(1) + Math.sin(theta1)).toFloat
      coords(2) = (coords(2) + Math.cos(theta2)).toFloat
      coords(3) = (coords(3) + Math.sin(theta2)).toFloat
      coords(4) = (coords(4) + Math.cos(theta3)).toFloat
      coords(5) = (coords(5) + Math.sin(theta3)).toFloat

      coords
    }

    spark.createDataset(points)
  }

}

