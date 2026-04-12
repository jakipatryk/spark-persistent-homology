package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.LocalPivotChunksStatistics
import org.apache.spark.sql.Dataset
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen

class CoboundaryMatrixReducerSpec
    extends AnyFlatSpec
    with SharedSparkContext
    with ScalaCheckPropertyChecks {

  behavior of "reduce"

  it should "always produce unique pivots for random point clouds and varying dimensions" in {
    import spark.implicits._

    val pointCloudGen = for {
      numPoints <- Gen.choose(2, 80)
      dim       <- Gen.choose(2, 4)
      points    <- Gen.listOfN(numPoints, Gen.listOfN(dim, Gen.choose(-10.0f, 10.0f)))
    } yield points.map(_.toArray).toArray

    val thresholdGen = Gen.choose(0.0f, 20.0f)
    val dimGen       = Gen.choose(0, 2)

    forAll(pointCloudGen, thresholdGen, dimGen) { (pointsCloud, distanceThreshold, dim) =>
      val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

      val numPoints = pointsCloud.length
      val cns       = CombinatorialNumberSystem(numPoints, dim + 2)

      implicit val context: FiltrationContext = FiltrationContext(
        sparkContext.broadcast(cns),
        sparkContext.broadcast(pointsCloud),
        distanceCalculator,
        distanceThreshold
      )

      val numSimplicesNextDim =
        cns.allCombinationsCount(Simplex.dimToCombinationSize((dim + 1).toByte))
      val accumulator =
        new PivotChunksStatisticsAccumulator(
          new LocalPivotChunksStatistics(10L, numSimplicesNextDim)
        )
      sparkContext.register(accumulator)

      // Generate unreduced matrix for dimension `dim`
      val unreducedMatrix = CoboundaryMatrixConstructor.construct(dim.toByte, accumulator, None)

      // Force evaluation and accumulator update before reduction,
      // just like it happens in the real pipeline.
      unreducedMatrix.count()

      val reducedMatrix = CoboundaryMatrixReducer.reduce(unreducedMatrix, accumulator)
      val results       = reducedMatrix.collect()
      val pivots        = results.flatMap(_.pivot)

      // Check that all non-empty pivots are unique
      assert(pivots.length == pivots.toSet.size)
    }
  }

}
