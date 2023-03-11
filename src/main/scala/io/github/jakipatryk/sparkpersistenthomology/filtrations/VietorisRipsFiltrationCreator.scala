package io.github.jakipatryk.sparkpersistenthomology.filtrations

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator.EuclideanDistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.{Chain, PointsCloud}
import io.github.jakipatryk.sparkpersistenthomology.distances.{DistanceCalculator, DistanceMatrix}
import io.github.jakipatryk.sparkpersistenthomology.utils.CombinatorialNumberSystemOnSteroids
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object VietorisRipsFiltrationCreator extends FiltrationCreator {

  override def createFiltration(
                                 pointsCloud: PointsCloud,
                                 maxDim: Option[Int] = None,
                                 distanceCalculator: DistanceCalculator = EuclideanDistanceCalculator
                               )(implicit sparkContext: SparkContext): Filtration = {
    pointsCloud.rdd.cache()

    val distanceMatrix = DistanceMatrix.fromPointsCloud(pointsCloud)
    val distanceMatrixBroadcasted = sparkContext.broadcast(distanceMatrix)

    val combinationElementsSetSize = pointsCloud.rdd.count().toInt
    val maxCombinationSize = maxDim.getOrElse(pointsCloud.rdd.collect().head.length - 1) + 1
    val system = new CombinatorialNumberSystemOnSteroids(
      combinationElementsSetSize,
      maxCombinationSize
    )
    val systemBroadcasted = sparkContext.broadcast(system)

    val range = sparkContext.range(0, system.allCombinationsCount)
    val initAndPoints = range.mapPartitions(
      it => {
        if(it.hasNext) {
          val startIndex = it.next()
          val combinationsIterator = systemBroadcasted.value.combinationsIterator(startIndex)
          val itRecovered = Iterator.single(startIndex) ++ it
          itRecovered.zip(combinationsIterator).map {
            case (_, combination) => combination.length match {
              case 1 =>
                (0.0, combination.map(_.toLong).toList)
              case _ =>
                (
                  combination
                    .combinations(2)
                    .map(a => distanceMatrixBroadcasted.value.getDistance(a(0), a(1)))
                    .max,
                  combination.map(_.toLong).toList
                )
            }
          }
        } else Iterator.empty
      }
    )

    val pointsDefiningBoundaryRDD = initAndPoints
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
