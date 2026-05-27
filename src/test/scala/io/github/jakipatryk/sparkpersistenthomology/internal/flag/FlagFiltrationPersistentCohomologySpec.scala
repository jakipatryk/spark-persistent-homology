package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.{
  FiltrationConfig,
  PersistencePair,
  SharedSparkContext
}

import scala.io.Source
import scala.util.Random
import scala.collection.mutable

class FlagFiltrationPersistentCohomologySpec extends AnyFlatSpec with SharedSparkContext {

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

  def loadExpectedPairs(path: String, dim: Int): Seq[PersistencePair] = {
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
      .toSeq
  }

  /** Compares two collections of persistence pairs using a "fuzzy set" approach. It ensures that
    * for every pair in one collection, there is a unique matching pair in the other within the
    * specified tolerance.
    */
  def assertPersistenceDiagramsMatch(
    expected: Seq[PersistencePair],
    actual: Seq[PersistencePair],
    tolerance: Float = 5e-4f
  ): Unit = {
    if (expected.size != actual.size) {
      fail(s"Diagram size mismatch: expected ${expected.size}, but got ${actual.size}")
    }

    val expSorted = expected.sortBy(p => (p.dim, p.birth, p.death))
    val actSorted = actual.sortBy(p => (p.dim, p.birth, p.death))

    val actMatched = new Array[Boolean](actSorted.size)
    val missing    = mutable.ArrayBuffer.empty[PersistencePair]

    var actPtr = 0
    for (exp <- expSorted) {
      // Fast-forward actPtr to the first potential match for this birth value
      while (
        actPtr < actSorted.size && (actSorted(actPtr).dim < exp.dim || (actSorted(
          actPtr
        ).dim == exp.dim && actSorted(actPtr).birth < exp.birth - tolerance))
      ) {
        actPtr += 1
      }

      var found = false
      var i     = actPtr
      // Search window for birth tolerance
      while (
        i < actSorted.size && actSorted(i).dim == exp.dim && actSorted(
          i
        ).birth <= exp.birth + tolerance && !found
      ) {
        if (!actMatched(i)) {
          val act = actSorted(i)
          val deathMatch =
            if (exp.death.isInfinity) act.death.isInfinity
            else !act.death.isInfinity && Math.abs(exp.death - act.death) < tolerance
          if (deathMatch) {
            actMatched(i) = true
            found = true
          }
        }
        i += 1
      }

      if (!found) missing.append(exp)
    }

    if (missing.nonEmpty) {
      val extra = actSorted.zipWithIndex.filter { case (_, idx) => !actMatched(idx) }.map(_._1)
      val msg = s"""Diagrams do not match within tolerance $tolerance.
                   |Total missing: ${missing.size}
                   |First 5 missing: ${missing.take(5).mkString(", ")}
                   |First 5 extra: ${extra.take(5).mkString(", ")}""".stripMargin
      fail(msg)
    }
  }

  it should "compute persistence pairs correctly for three_spheres point cloud up to maxDim=2 (result from ripser)" in {
    val pointsCloud = spark.createDataset(loadPointsCloud("/three_spheres/points_cloud.csv"))
    val maxDim      = 2

    val expectedDim0 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_0.csv", 0)
    val expectedDim1 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_1.csv", 1)
    val expectedDim2 = loadExpectedPairs("/three_spheres/persistence_pairs_dim_2.csv", 2)

    val results = FlagFiltrationPersistentCohomology.computePersistencePairs(
      pointsCloud,
      maxDim,
      FiltrationConfig.VietorisRips()
    )

    assert(results.length == 3)

    assertPersistenceDiagramsMatch(expectedDim0, results(0).collect().toSeq)
    assertPersistenceDiagramsMatch(expectedDim1, results(1).collect().toSeq)
    assertPersistenceDiagramsMatch(expectedDim2, results(2).collect().toSeq)
  }

  it should "compute persistence pairs correctly for a noisy high-dimensional (10D) point cloud (5,000 points, 4 clusters)" in {
    val pointsCloud = spark.createDataset(loadPointsCloud("/noisy_clusters/points_cloud.csv"))
    val maxDim      = 2
    val threshold   = Some(2.661869f)

    val expectedDim0 = loadExpectedPairs("/noisy_clusters/persistence_pairs_dim_0.csv", 0)
    val expectedDim1 = loadExpectedPairs("/noisy_clusters/persistence_pairs_dim_1.csv", 1)
    val expectedDim2 = loadExpectedPairs("/noisy_clusters/persistence_pairs_dim_2.csv", 2)

    val results = FlagFiltrationPersistentCohomology.computePersistencePairs(
      pointsCloud,
      maxDim,
      FiltrationConfig.VietorisRips(distanceThreshold = threshold)
    )

    assert(results.length == 3)

    assertPersistenceDiagramsMatch(expectedDim0, results(0).collect().toSeq)
    assertPersistenceDiagramsMatch(expectedDim1, results(1).collect().toSeq)
    assertPersistenceDiagramsMatch(expectedDim2, results(2).collect().toSeq)
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
    val results = FlagFiltrationPersistentCohomology.computePersistencePairs(
      pointsCloud,
      maxDim,
      FiltrationConfig.VietorisRips()
    )

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

    val results = FlagFiltrationPersistentCohomology.computePersistencePairs(
      pointsCloud,
      maxDim,
      FiltrationConfig.VietorisRips()
    )

    val dim0Pairs     = results(0).collect()
    val infinitePairs = dim0Pairs.filter(_.death.isInfinity)

    assert(
      infinitePairs.length == 1,
      s"Expected exactly 1 infinite persistence pair in dim 0, but got ${infinitePairs.length}. Total dim 0 pairs: ${dim0Pairs.length}"
    )
  }

}
