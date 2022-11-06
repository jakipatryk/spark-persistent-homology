package com.jakipatryk.spark.persistenthomology

import com.jakipatryk.spark.persistenthomology.filtrations.{
  Filtration,
  FiltrationCreator,
  IndexInMatrix,
  InitThreshold,
  PointsCloud,
  SimplexBoundary,
  VietorisRipsFiltrationCreator
}
import com.jakipatryk.spark.persistenthomology.matrixreduction.{BoundaryMatrix, BoundaryMatrixReduction}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

trait Infinity
object Infinity extends Infinity with Serializable

case class PersistencePair(birth: Double, death: Either[Double, Infinity], dim: Int)

object PersistentHomology {

  /**
   * Takes a points cloud and computes all persistence pairs (including infinite ones).
   * @param pointsCloud RDD of Vectors of Doubles representing points cloud
   * @param numOfPartitionsConf (default None) number of partitions to use when calculating persistent homology
   * @param maxDim (default None) max dimension of simplices in a filtration;
   *               dimension of a simplex = (number of points that define it) - 1
   * @param filtrationCreator (default VietorisRipsFiltrationCreator)
   * @return All (finite and infinite) persistence pairs
   */
  def getPersistencePairs(
                           pointsCloud: PointsCloud,
                           numOfPartitionsConf: Option[Int] = None,
                           maxDim: Option[Int] = None,
                           filtrationCreator: FiltrationCreator = VietorisRipsFiltrationCreator
                         ): RDD[PersistencePair] = {
    val filtration = filtrationCreator.createFiltration(pointsCloud, maxDim)

    numOfPartitionsConf match {
      case Some(n) => getPersistencePairs(filtration, n)
      case None => getPersistencePairs(filtration)
    }
  }

  /**
   * Takes a filtration and computes all persistence pairs (including infinite ones).
   *
   * @param filtration RDD representing filtration
   * @return All (finite and infinite) persistence pairs
   */
  def getPersistencePairs(filtration: Filtration): RDD[PersistencePair] = {
    val defaultPartitioner = Partitioner.defaultPartitioner(filtration.rdd)
    val numOfPartitions = defaultPartitioner.numPartitions

    getPersistencePairs(filtration, numOfPartitions)
  }

  /**
   * Takes a filtration and computes all persistence pairs (including infinite ones).
   *
   * @param filtration RDD representing filtration
   * @param numOfPartitions number of partitions to use when calculating persistent homology
   * @return All (finite and infinite) persistence pairs
   */
  def getPersistencePairs(filtration: Filtration, numOfPartitions: Int): RDD[PersistencePair] = {
    filtration.rdd.cache()
    val filtrationLength = filtration.rdd.count()
    val (boundaryMatrix, mapping) = filtrationToBoundaryMatrixAndThresholdMapping(filtration)

    val reducedMatrix = BoundaryMatrixReduction
      .reduceBoundaryMatrix(boundaryMatrix, numOfPartitions, filtrationLength)

    val finiteIndicesPairs = reducedMatrix.rdd.map {
      case (Key(indexInMatrix, Some(pivot)), _) => PersistenceIndicesPair(pivot, Left(indexInMatrix))
    }
    val infiniteIndicesPairs = getInfinitePairs(
      finiteIndicesPairs, filtrationLength, numOfPartitions
    )

    indicesPairsToThresholdPairs(finiteIndicesPairs, infiniteIndicesPairs, mapping)
      .filter {
        case PersistencePair(_, Right(_), _) => true
        case PersistencePair(birth, Left(death), _) => birth != death
      }
  }

  private[persistenthomology] case class PersistenceIndicesPair(
                                                                 birthIndex: Long,
                                                                 deathIndex: Either[Long, Infinity]
                                                               )

  private[this] def filtrationToBoundaryMatrixAndThresholdMapping(
                                                     filtration: Filtration
                                                   ): (BoundaryMatrix, RDD[(Long, (Double, Int))]) = {
    val boundaryMatrix = filtration
      .rdd
      .map { case (IndexInMatrix(index), _, SimplexBoundary(boundary)) => (Key(index, boundary.pivot), boundary) }
    val mapping = filtration
      .rdd
      .map { case (IndexInMatrix(index), InitThreshold(threshold), SimplexBoundary(boundary)) =>
        (index, (threshold, boundary.asVector.indicesOfOnes.length match { case 0 => 0 case d => d - 1 }))
      }
    (BoundaryMatrix(boundaryMatrix), mapping)
  }

  private[this] class IndicesPartitioner(
                                          val numPartitions: Int,
                                          val filtrationLength: Long
                                        ) extends Partitioner {

    override def getPartition(key: Any): Int = key match {
      case key: Long => Math.floor(
        key / Math.ceil(filtrationLength.toFloat / numPartitions)
      ).toInt
    }

  }

  /** Generates infinite pairs based on finite pairs and filtration length. */
  private[persistenthomology] def getInfinitePairs(
                        finitePairs: RDD[PersistenceIndicesPair],
                        filtrationLength: Long,
                        numOfPartitions: Int
                      ): RDD[PersistenceIndicesPair] = {
    val allFiniteIndices = finitePairs flatMap {
      case PersistenceIndicesPair(birthIndex, Left(deathIndex)) =>
        (birthIndex, ()) :: (deathIndex, ()) :: Nil
    }

    val partitioner = new IndicesPartitioner(numOfPartitions, filtrationLength)
    val partitionRangeLength = Math.ceil(filtrationLength.toDouble / numOfPartitions).toLong

    allFiniteIndices
      .repartitionAndSortWithinPartitions(partitioner)
      .mapPartitionsWithIndex {
        (index, partition) => {
          val range = (
            index * partitionRangeLength,
            Math.min((index + 1) * partitionRangeLength - 1, filtrationLength - 1)
          )

          val missingIndices = partition.foldLeft((range._1 - 1, List[Long]())) {
            case ((prevElement, acc), (i, _)) => (i, ((prevElement + 1) until i).reverse.toList ++ acc)
          }

          (missingIndices._2 ++ ((missingIndices._1 + 1) to range._2).toList)
            .map { i => PersistenceIndicesPair(i, Right(Infinity)) }
            .iterator
        }
      }
  }

  private[this] def indicesPairsToThresholdPairs(
                                                  finiteIndicesPairs: RDD[PersistenceIndicesPair],
                                                  infiniteIndicesPairs: RDD[PersistenceIndicesPair],
                                                  mapping: RDD[(Long, (Double, Int))]
                                                ): RDD[PersistencePair] = {
    val finitePairs = finiteIndicesPairs.map {
      case PersistenceIndicesPair(birthIndex, deathIndex) => (birthIndex, deathIndex)
    }
      .join(mapping)
      .map { case (_, (Left(deathIndex), (birthThreshold, dim))) => (deathIndex, (birthThreshold, dim)) }
      .join(mapping)
      .map {
        case (_, ((birthThreshold, dim), (deathThreshold, _))) =>
          PersistencePair(birthThreshold, Left(deathThreshold), dim)
      }
    val infinitePairs = infiniteIndicesPairs.map {
      case PersistenceIndicesPair(birthIndex, _) => (birthIndex, ())
    }
      .join(mapping)
      .map { case (_, (_, (threshold, dim))) => PersistencePair(threshold, Right(Infinity), dim) }

    finitePairs union infinitePairs
  }

}
