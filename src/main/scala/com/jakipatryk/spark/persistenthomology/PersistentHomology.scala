package com.jakipatryk.spark.persistenthomology

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

trait Infinity
object Infinity extends Infinity with Serializable

case class PersistencePair(birthIndex: Long, deathIndex: Either[Long, Infinity])

class IndicesPartitioner(val numPartitions: Int, val filtrationLength: Long) extends Partitioner {

  override def getPartition(key: Any): Int = key match {
    case key: Long => Math.floor(
      key / Math.ceil(filtrationLength.toFloat / numPartitions)
    ).toInt
  }

}

object PersistentHomology {

  /** Takes a boundary matrix of a filtration and computes all persistence pairs (including infinite ones). */
  def getPersistencePairs(boundaryMatrix: RDD[(Key, Chain)], numOfPartitions: Int): RDD[PersistencePair] = {
    val filtrationLength = boundaryMatrix.count()
    val reducedMatrix = BoundaryMatrixReduction.reduceBoundaryMatrix(boundaryMatrix, numOfPartitions)

    val finitePairs = reducedMatrix map {
      case (Key(indexInMatrix, Some(pivot)), _) => PersistencePair(pivot, Left(indexInMatrix))
    }
    val infinitePairs = getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)

    finitePairs union infinitePairs
  }

  /** Generates infinite pairs based on finite pairs and filtration length. */
  def getInfinitePairs(
                        finitePairs: RDD[PersistencePair],
                        filtrationLength: Long,
                        numOfPartitions: Int): RDD[PersistencePair] = {
    val allFiniteIndices = finitePairs flatMap {
      case PersistencePair(birthIndex, Left(deathIndex)) => (birthIndex, ()) :: (deathIndex, ()) :: Nil
    }
    val partitioner = new IndicesPartitioner(numOfPartitions, filtrationLength)
    val partitionRangeLength = Math.ceil(filtrationLength.toDouble / numOfPartitions).toLong
    val infinitePairs = allFiniteIndices
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
            .map { i => PersistencePair(i, Right(Infinity)) }
            .iterator
        }
      }
    infinitePairs
  }

}
