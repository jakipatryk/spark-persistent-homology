package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{ PersistencePair, SharedSparkContext }

import scala.io.Source
import scala.util.Random

class VietorisRipsPersistentCohomologySpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "computePersistencePairs"

  def loadPointsCloud(path: String): Seq[Array[Float]] = {
    val source  = Source.fromURL(getClass.getResource(path))
    val content = source.mkString
    source.close()
    content
      .split("\n")
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.toFloat))
      .toSeq
  }

  def loadExpectedPairs(path: String, dim: Int): Set[PersistencePair] = {
    val source  = Source.fromURL(getClass.getResource(path))
    val content = source.mkString
    source.close()
    content
      .split("\n")
      .filter(_.nonEmpty)
      .map { line =>
        val parts = line.split(",")
        val birth = parts(0).toFloat
        val death =
          if (parts(1).equalsIgnoreCase("inf")) PersistencePair.Infinity else parts(1).toFloat
        PersistencePair(dim, birth, death)
      }
      .toSet
  }

  def areSetsAlmostEqual(
    expected: Set[PersistencePair],
    actual: Set[PersistencePair],
    tolerance: Float = 1e-5f
  ): Boolean = {
    if (expected.size != actual.size) return false

    expected.forall { exp =>
      actual.exists { act =>
        act.dim == exp.dim &&
        (if (exp.death.isInfinity) act.death.isInfinity
         else Math.abs(act.death - exp.death) < tolerance) &&
        Math.abs(act.birth - exp.birth) < tolerance
      }
    }
  }

  it should "compute persistence pairs correctly for three_spheres point cloud up to maxDim=2 (result from ripser)" in {
    val pointsCloud = spark.createDataset(loadPointsCloud("/three_spheres/points_cloud.csv"))
    val maxDim      = 2

    val expectedDim0 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_0.csv", 0)
    val expectedDim1 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_1.csv", 1)
    val expectedDim2 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_2.csv", 2)

    val results = VietorisRipsPersistentCohomology.computePersistencePairs(pointsCloud, maxDim)

    assert(results.length == 3)

    val actualDim0 = results(0).collect().toSet
    val actualDim1 = results(1).collect().toSet
    val actualDim2 = results(2).collect().toSet

    def printMissing(
      expected: Set[PersistencePair],
      actual: Set[PersistencePair],
      name: String
    ): Unit = {
      val missing = expected.filter(exp =>
        !actual.exists { act =>
          act.dim == exp.dim &&
          (if (exp.death.isInfinity) act.death.isInfinity
           else Math.abs(act.death - exp.death) < 1e-5f) &&
          Math.abs(act.birth - exp.birth) < 1e-5f
        }
      )
      if (missing.nonEmpty) {
        println(s"Missing in $name: ${missing.take(5).mkString(", ")} (total ${missing.size})")
        val extra = actual.filter(act =>
          !expected.exists { exp =>
            act.dim == exp.dim &&
            (if (exp.death.isInfinity) act.death.isInfinity
             else Math.abs(act.death - exp.death) < 1e-5f) &&
            Math.abs(act.birth - exp.birth) < 1e-5f
          }
        )
        println(s"Extra in $name: ${extra.take(5).mkString(", ")} (total ${extra.size})")
      }
    }

    printMissing(expectedDim0, actualDim0, "Dim 0")
    printMissing(expectedDim1, actualDim1, "Dim 1")
    printMissing(expectedDim2, actualDim2, "Dim 2")

    assert(
      areSetsAlmostEqual(expectedDim0, actualDim0),
      s"Dim 0 pairs do not match. Expected size: ${expectedDim0.size}, Actual size: ${actualDim0.size}"
    )
    assert(
      areSetsAlmostEqual(expectedDim1, actualDim1),
      s"Dim 1 pairs do not match. Expected size: ${expectedDim1.size}, Actual size: ${actualDim1.size}"
    )
    assert(
      areSetsAlmostEqual(expectedDim2, actualDim2),
      s"Dim 2 pairs do not match. Expected size: ${expectedDim2.size}, Actual size: ${actualDim2.size}"
    )
  }

  it should "find exactly one persistence pair of any kind in dim 1 for 150 equilateral triangles placed evenly on a circle" in {
    val numTriangles = 150
    val circleRadius = 100.0
    val triangleSide = 0.01f
    val h            = (triangleSide * math.sqrt(3.0) / 2.0).toFloat

    val points = (0 until numTriangles).flatMap { i =>
      val theta = 2 * math.Pi * i / numTriangles
      val cx    = (circleRadius * math.cos(theta)).toFloat
      val cy    = (circleRadius * math.sin(theta)).toFloat

      val p1 = Array(0.0f, 2.0f * h / 3.0f)
      val p2 = Array(-triangleSide / 2.0f, -h / 3.0f)
      val p3 = Array(triangleSide / 2.0f, -h / 3.0f)

      Seq(
        Array(cx + p1(0), cy + p1(1)),
        Array(cx + p2(0), cy + p2(1)),
        Array(cx + p3(0), cy + p3(1))
      )
    }

    val pointsCloud = spark.createDataset(points)
    val maxDim      = 1
    val results     = VietorisRipsPersistentCohomology.computePersistencePairs(pointsCloud, maxDim)

    val dim1Pairs = results(1).collect()

    assert(
      dim1Pairs.length == 1,
      s"Expected exactly 1 persistence pair in dim 1, but got ${dim1Pairs.length}"
    )
  }

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
