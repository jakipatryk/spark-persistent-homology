package com.jakipatryk.spark.persistenthomology

import org.scalatest.flatspec.AnyFlatSpec

class InitialPartitionerSpec extends AnyFlatSpec {

  "getPartition" should "return correct partition index (odd numPartitions, even filtrationLength)" in {
    val partitioner: InitialPartitioner = new InitialPartitioner(3, 10)

    assert(partitioner.getPartition(Key(0, None)) == 0)
    assert(partitioner.getPartition(Key(1, None)) == 0)
    assert(partitioner.getPartition(Key(2, None)) == 0)
    assert(partitioner.getPartition(Key(3, None)) == 0)
    assert(partitioner.getPartition(Key(4, None)) == 1)
    assert(partitioner.getPartition(Key(5, None)) == 1)
    assert(partitioner.getPartition(Key(6, None)) == 1)
    assert(partitioner.getPartition(Key(7, None)) == 1)
    assert(partitioner.getPartition(Key(8, None)) == 2)
    assert(partitioner.getPartition(Key(9, None)) == 2)
  }

  "getPartition" should "return correct partition index (even numPartitions, even filtrationLength)" in {
    val partitioner: InitialPartitioner = new InitialPartitioner(4, 10)

    assert(partitioner.getPartition(Key(0, None)) == 0)
    assert(partitioner.getPartition(Key(1, None)) == 0)
    assert(partitioner.getPartition(Key(2, None)) == 0)
    assert(partitioner.getPartition(Key(3, None)) == 1)
    assert(partitioner.getPartition(Key(4, None)) == 1)
    assert(partitioner.getPartition(Key(5, None)) == 1)
    assert(partitioner.getPartition(Key(6, None)) == 2)
    assert(partitioner.getPartition(Key(7, None)) == 2)
    assert(partitioner.getPartition(Key(8, None)) == 2)
    assert(partitioner.getPartition(Key(9, None)) == 3)
  }

  "getPartition" should "return correct partition index (numPartitions = filtrationLength)" in {
    val partitioner: InitialPartitioner = new InitialPartitioner(10, 10)

    assert(partitioner.getPartition(Key(0, None)) == 0)
    assert(partitioner.getPartition(Key(1, None)) == 1)
    assert(partitioner.getPartition(Key(2, None)) == 2)
    assert(partitioner.getPartition(Key(3, None)) == 3)
    assert(partitioner.getPartition(Key(4, None)) == 4)
    assert(partitioner.getPartition(Key(5, None)) == 5)
    assert(partitioner.getPartition(Key(6, None)) == 6)
    assert(partitioner.getPartition(Key(7, None)) == 7)
    assert(partitioner.getPartition(Key(8, None)) == 8)
    assert(partitioner.getPartition(Key(9, None)) == 9)
  }

  "getPartition" should "return correct partition index (filtrationLength divisible by numPartitions)" in {
    val partitioner: InitialPartitioner = new InitialPartitioner(4, 8)

    assert(partitioner.getPartition(Key(0, None)) == 0)
    assert(partitioner.getPartition(Key(1, None)) == 0)
    assert(partitioner.getPartition(Key(2, None)) == 1)
    assert(partitioner.getPartition(Key(3, None)) == 1)
    assert(partitioner.getPartition(Key(4, None)) == 2)
    assert(partitioner.getPartition(Key(5, None)) == 2)
    assert(partitioner.getPartition(Key(6, None)) == 3)
    assert(partitioner.getPartition(Key(7, None)) == 3)
  }

  "getPartition" should "return correct partition index (numPartitions > filtrationLength)" in {
    val partitioner: InitialPartitioner = new InitialPartitioner(3, 2)

    assert(partitioner.getPartition(Key(0, None)) == 0)
    assert(partitioner.getPartition(Key(1, None)) == 1)
  }

}
