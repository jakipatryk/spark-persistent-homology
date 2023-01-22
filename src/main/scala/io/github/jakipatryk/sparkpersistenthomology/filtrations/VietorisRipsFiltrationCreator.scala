package io.github.jakipatryk.sparkpersistenthomology.filtrations

import io.github.jakipatryk.sparkpersistenthomology.Chain
import io.github.jakipatryk.sparkpersistenthomology.distances.{DistanceCalculator, EuclideanDistanceCalculator}
import io.github.jakipatryk.sparkpersistenthomology.utils.CombinatorialUtils
import io.github.jakipatryk.sparkpersistenthomology.distances.EuclideanDistanceCalculator
import org.apache.spark.rdd.RDD

object VietorisRipsFiltrationCreator extends FiltrationCreator {

  override def createFiltration(
                                 pointsCloud: PointsCloud,
                                 maxDim: Option[Int] = None,
                                 distanceCalculator: DistanceCalculator = EuclideanDistanceCalculator
                               ): Filtration = {
    val maxNumberOfPointsInSimplex = maxDim.getOrElse(pointsCloud.rdd.first().length - 1) + 1

    val pointsDefiningBoundaryRDD = CombinatorialUtils
      .computeForAllCombinationsUpToN(
        pointsCloud.rdd, maxNumberOfPointsInSimplex, computeThreshold(distanceCalculator)
      )
      .sortBy { case (initThreshold, points) => (initThreshold, points.length) }
      .zipWithIndex()
      .map {
        case ((initThreshold, pointsDefiningBoundary), index) =>
          (pointsDefiningBoundary, (index, initThreshold))
      }

    Filtration(
      pointsDefiningSimplexToBoundaryChain(pointsDefiningBoundaryRDD)
    )
  }

  private[this] def computeThreshold(distanceCalculator: DistanceCalculator)
                                                  (combination: List[(Vector[Double], Long)]): Double =
    combination match {
      case _ :: Nil => 0.0
      case _ =>
        val vectors = combination.map { case (v, _) => v }
        vectors
          .combinations(2)
          .map { case v1 :: v2 :: Nil => distanceCalculator.calculateDistance(v1, v2) }
          .max
    }

  private[this] def pointsDefiningSimplexToBoundaryChain(
                                                          pointsRDD: RDD[(List[Long], (Long, Double))]
                                                        ): RDD[(IndexInMatrix, InitThreshold, SimplexBoundary)] = {
    val boundariesDefinedByPoints = pointsRDD.flatMap {
      case (points, (index, threshold)) =>
        val dim = points.length
        points.combinations(dim - 1).map(l => (l, (index, threshold)))
    }

    val dimOneAndAbove = boundariesDefinedByPoints.filter(_._1.nonEmpty)

    dimOneAndAbove
      .join(pointsRDD.map { case (l, (i, _)) => (l, i) })
      .map {
        case (_, ((indexOfSimplex, threshold), indexOfBoundarySimplex)) =>
          ((indexOfSimplex, threshold), indexOfBoundarySimplex)
      }
      .groupByKey()
      .map {
        case ((index, threshold), boundary) =>
          (
            IndexInMatrix(index),
            InitThreshold(threshold),
            SimplexBoundary(Chain(boundary.toList.sorted(Ordering[Long].reverse)))
          )
      }
      .union(
        boundariesDefinedByPoints
          .filter(_._1.isEmpty)
          .map {
            case (l, (i, t)) =>
              (
                IndexInMatrix(i),
                InitThreshold(t),
                SimplexBoundary(Chain(l))
              )
          }
      )
  }

}
