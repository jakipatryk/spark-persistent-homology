package com.jakipatryk.spark.persistenthomology

import org.scalatest.flatspec.AnyFlatSpec

class PivotPartitionerSpec extends AnyFlatSpec {

  "getPartition" should "return correct partition index" in {
    val partitioner = new PivotPartitioner(3, 10)

    assert(partitioner.getPartition(Key(0, Some(0))) == 0)
    assert(partitioner.getPartition(Key(1, Some(1))) == 0)
    assert(partitioner.getPartition(Key(2, Some(0))) == 0)
    assert(partitioner.getPartition(Key(3, Some(0))) == 0)
    assert(partitioner.getPartition(Key(4, Some(0))) == 0)
    assert(partitioner.getPartition(Key(5, Some(0))) == 0)
    assert(partitioner.getPartition(Key(6, Some(3))) == 0)
    assert(partitioner.getPartition(Key(7, Some(4))) == 1)
    assert(partitioner.getPartition(Key(8, Some(5))) == 1)
    assert(partitioner.getPartition(Key(9, Some(8))) == 2)
  }

}
