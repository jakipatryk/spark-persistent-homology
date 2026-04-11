package io.github.jakipatryk.sparkpersistenthomology

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.Source

class PersistentHomologySpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  behavior of "computePersistentHomology"

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

  it should "compute persistence pairs correctly for three_spheres point cloud up to maxDim=2" in {
    val pointsCloud = spark.createDataset(loadPointsCloud("/three_spheres/points_cloud.csv"))
    val maxDim      = 2

    val expectedDim0 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_0.csv", 0)
    val expectedDim1 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_1.csv", 1)
    val expectedDim2 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_2.csv", 2)

    val results = PersistentHomology.computePersistentHomology(pointsCloud, maxDim)

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

}
