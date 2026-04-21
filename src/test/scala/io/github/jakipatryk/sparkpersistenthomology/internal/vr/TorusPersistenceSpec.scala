package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ PersistencePair, SharedSparkContext }
import scala.util.Random

class TorusPersistenceSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "computePersistencePairs on Torus"

  private def generateTorusPoints(numPoints: Int): Dataset[Array[Float]] = {
    val random     = new Random(42)
    val R          = 2.0
    val r          = 1.0
    val noiseScale = 0.05

    val points = (1 to numPoints).map { _ =>
      val theta  = random.nextDouble() * 2.0 * Math.PI
      val phi    = random.nextDouble() * 2.0 * Math.PI
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

  it should "find exactly one infinite persistence pair in dim 0 for 2000 points on a torus" in {
    val numPoints   = 2000
    val pointsCloud = generateTorusPoints(numPoints)
    val maxDim      = 0

    val results = VietorisRipsPersistentCohomology.computePersistencePairs(pointsCloud, maxDim)

    val dim0Pairs     = results(0).collect()
    val infinitePairs = dim0Pairs.filter(_.death.isInfinity)

    assert(
      infinitePairs.length == 1,
      s"Expected exactly 1 infinite persistence pair in dim 0, but got ${infinitePairs.length}. Total dim 0 pairs: ${dim0Pairs.length}"
    )
  }

}
