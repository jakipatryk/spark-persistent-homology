package com.jakipatryk.spark.persistenthomology

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec


class BoundaryMatrixReductionSpec extends AnyFlatSpec with DataLoader with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("BoundaryMatrixReductionSpec").setMaster("local[*]")
    )
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "reduceBlock" should "make matrix reduced if the entire boundary matrix is given and block range is whole matrix" in {
    val data = nSeparateTriangles(3)

    val result = BoundaryMatrixReduction.reduceBlock(data, (0L, 20L), (0L, 20L))
      .toList
      .sortBy { case (k, _) => k.indexInMatrix }

    // 0-dimensional homology classes are born when points are added to the filtration
    assert(result.slice(0, 9).forall { case (k, c) => k.pivot.isEmpty && c.isEmpty })

    // newer 0-dimensional homology classes die when points get connected
    assert(result(9)._1.pivot.contains(1) && result(9)._2.asVector.indicesOfOnes == (1L :: 0L :: Nil))
    assert(result(10)._1.pivot.contains(4) && result(10)._2.asVector.indicesOfOnes == (4L :: 3L :: Nil))
    assert(result(11)._1.pivot.contains(7) && result(11)._2.asVector.indicesOfOnes == (7L :: 6L :: Nil))
    assert(result(12)._1.pivot.contains(2) && result(12)._2.asVector.indicesOfOnes == (2L :: 1L :: Nil))
    assert(result(13)._1.pivot.contains(5) && result(13)._2.asVector.indicesOfOnes == (5L :: 4L :: Nil))
    assert(result(14)._1.pivot.contains(8) && result(14)._2.asVector.indicesOfOnes == (8L :: 7L :: Nil))

    // 1-dimensional homology classes are born when line segments start forming a boundary
    assert(result(15)._1.pivot.isEmpty && result(15)._2.isEmpty)
    assert(result(16)._1.pivot.isEmpty && result(16)._2.isEmpty)
    assert(result(17)._1.pivot.isEmpty && result(17)._2.isEmpty)

    // 1-dimensional homology classes die when triangles get filled
    assert(result(18)._1.pivot.contains(15) && result(18)._2.asVector.indicesOfOnes == (15L :: 12L :: 9L :: Nil))
    assert(result(19)._1.pivot.contains(16) && result(19)._2.asVector.indicesOfOnes == (16L :: 13L :: 10L :: Nil))
    assert(result(20)._1.pivot.contains(17) && result(20)._2.asVector.indicesOfOnes == (17L :: 14L :: 11L :: Nil))
  }

  "reduceBlock" should "reduce columns in the block (no two have the same pivot in the block row range)" in {
    val columnsInPartition = (
      (Key(4L, Some(2L)), Chain(2L :: Nil))
        :: (Key(8L, Some(3L)), Chain(3L :: 2L :: Nil))
        :: (Key(15L, Some(3L)), Chain(3L :: Nil))
        :: (Key(17L, Some(4L)), Chain(4L :: 3L :: 2L :: Nil))
        :: (Key(19L, Some(2L)), Chain(2L :: Nil))
        :: (Key(24L, Some(3L)), Chain(3L :: 2L :: Nil))
        :: Nil).iterator

    val result = BoundaryMatrixReduction.reduceBlock(columnsInPartition, (10L, 20L), (2L, 4L))
      .toList
      .sortBy { case (k, _) => k.indexInMatrix }

    // these were already reduced, because they have index less than left boundary of the current block column range
    assert(result(0) == (Key(4L, Some(2)), Chain(2L :: Nil)))
    assert(result(1) == (Key(8L, Some(3)), Chain(3L :: 2L :: Nil)))

    // these should have been reduced by the algorithm
    assert(result(2) == (Key(15L, None), Chain(Empty)))
    assert(result(3) == (Key(17L, Some(4L)), Chain(4L :: 3L :: 2L :: Nil)))
    assert(result(4) == (Key(19L, None), Chain(Empty)))

    // this should not have been reduced, because they have index greater than the current block column range
    assert(result(5) == (Key(24L, Some(3L)), Chain(3L :: 2L :: Nil)))
  }

  "reduceBoundaryMatrix" should "reduce boundary matrix of 10 triangles with 3 partitions correctly" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)
    val filtrationLength = data.count()

    val result =
      BoundaryMatrixReduction
        .reduceBoundaryMatrix(data, 3, filtrationLength)
        .collect()
        .toList
        .sortBy { case (k, _) => k.indexInMatrix }

    assert(result == nSeparateTrianglesExpectedReducedMatrix(10))
  }

  "reduceBoundaryMatrix" should "reduce boundary matrix of 10 triangles with 8 partitions correctly" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)
    val filtrationLength = data.count()

    val result =
      BoundaryMatrixReduction
        .reduceBoundaryMatrix(data, 8, filtrationLength)
        .collect()
        .toList
        .sortBy { case (k, _) => k.indexInMatrix }

    assert(result == nSeparateTrianglesExpectedReducedMatrix(10))
  }

}
