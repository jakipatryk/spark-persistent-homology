package com.jakipatryk.spark.persistenthomology

import org.apache.spark.Partitioner

class PivotPartitioner(val numPartitions: Int, val filtrationLength: Long) extends Partitioner {

  /**
   * Partitioner used during iteration of PH algorithm.
   * Assumes there are no zero columns (they should be filtered out before repartitioning).
   */
  override def getPartition(key: Any): Int = key match {
    case Key(_, Some(pivot)) => Math.floor(
        pivot / Math.ceil(filtrationLength.toFloat / numPartitions)
      ).toInt
  }

}
