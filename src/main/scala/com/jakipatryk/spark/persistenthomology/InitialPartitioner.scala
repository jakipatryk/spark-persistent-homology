package com.jakipatryk.spark.persistenthomology

import org.apache.spark.Partitioner

class InitialPartitioner(val numPartitions: Int, val filtrationLength: Long) extends Partitioner {
  override def getPartition(key: Any): Int = key match {
    case Key(indexInMatrix, _) => Math.floor(
      indexInMatrix / Math.ceil(filtrationLength.toFloat / numPartitions)
    ).toInt
  }
}
