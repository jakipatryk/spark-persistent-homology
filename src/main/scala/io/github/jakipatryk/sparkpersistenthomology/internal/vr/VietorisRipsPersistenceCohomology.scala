package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import org.apache.spark.sql.SparkSession
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

private[sparkpersistenthomology] object VietorisRipsPersistentCohomology {

  def computePersistencePairs(
    pointsCloud: Dataset[Array[Float]],
    maxDim: Int,
    distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator,
    distanceThreshold: Option[Float] = None
  )(implicit spark: SparkSession): Array[Dataset[PersistencePair]] = {
    import spark.implicits._

    val pointsArr = pointsCloud.collect()
    val numPoints = pointsArr.length

    if (numPoints == 0) {
      return Array.fill(maxDim + 1)(spark.emptyDataset[PersistencePair])
    }

    val maxCombinationSize = Simplex.dimToCombinationSize(maxDim.toByte) + 1
    val cns                = new CombinatorialNumberSystem(numPoints, maxCombinationSize)
    val cnsBroadcast       = spark.sparkContext.broadcast(cns)
    val pointsBroadcast    = spark.sparkContext.broadcast(pointsArr)
    val threshold = distanceThreshold.getOrElse(
      EnclosingRadiusCalculator.computeRadius(pointsCloud, pointsBroadcast, distanceCalculator)
    )

    implicit val context: FiltrationContext = FiltrationContext(
      cnsBroadcast,
      pointsBroadcast,
      distanceCalculator,
      threshold
    )

    var previousDimResult: Option[Dataset[CoboundaryMatrixColumn]] = None
    val results = new Array[Dataset[PersistencePair]](maxDim + 1)

    for (dim <- 0 to maxDim) {
      val d              = dim.toByte
      val numberOfPivots = cns.allCombinationsCount(Simplex.dimToCombinationSize((d + 1).toByte))

      val numPartitions = spark.conf.get("spark.sql.shuffle.partitions").toInt
      val chunkSize     = math.max(1L, numberOfPivots / (numPartitions * 10))

      val localStats =
        new PivotChunksStatisticsAccumulator.LocalPivotChunksStatistics(chunkSize, numberOfPivots)
      val accumulator = new PivotChunksStatisticsAccumulator(localStats)
      spark.sparkContext.register(accumulator, s"PivotStats_dim_$d")

      val matrix        = CoboundaryMatrixConstructor.construct(d, accumulator, previousDimResult)
      val reducedMatrix = CoboundaryMatrixReducer.reduce(matrix, accumulator)

      val cachedReducedMatrix = reducedMatrix.localCheckpoint()

      val pairs = cachedReducedMatrix.flatMap { col =>
        val x = col.initialSimplex.radius
        col.pivot match {
          case Some(pivotSimplex) =>
            val y = pivotSimplex.radius
            if (x != y) Some(PersistencePair(dim, x, y)) else None
          case None =>
            Some(PersistencePair(dim, x, PersistencePair.Infinity))
        }
      }

      results(dim) = pairs
      previousDimResult = Some(cachedReducedMatrix)
    }
    results
  }

}
