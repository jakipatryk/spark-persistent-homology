package io.github.jakipatryk.sparkpersistenthomology.matrixreduction

import io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators.{DefaultOrchestrator, Orchestrator}
import io.github.jakipatryk.sparkpersistenthomology.utils.Empty
import io.github.jakipatryk.sparkpersistenthomology.{Chain, Key}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec


class BoundaryMatrixReductionSpec extends AnyFlatSpec with DataLoader with BeforeAndAfterAll {

  implicit var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("BoundaryMatrixReductionSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "reduceBlock" should "make matrix reduced if the entire boundary matrix is given and block range is whole matrix" in {
    val data = nSeparateTriangles(3)

    val blockColumnRange = Orchestrator.Bounds(
      Orchestrator.BoundStartInclusive(0L),
      Orchestrator.BoundEndInclusive(20L),
    )
    val blockRowRange = Orchestrator.Bounds(
      Orchestrator.BoundStartInclusive(0L),
      Orchestrator.BoundEndInclusive(20L),
    )

    val result = BoundaryMatrixReduction.reduceBlock(data, blockColumnRange, blockRowRange)
      .toList
      .sortBy { case (k, _) => k.indexInMatrix }

    // newer 0-dimensional homology classes die when points get connected
    assert(result(0)._1.pivot.contains(1) && result(0)._2.asVector.indicesOfOnes == (1L :: 0L :: Nil))
    assert(result(1)._1.pivot.contains(4) && result(1)._2.asVector.indicesOfOnes == (4L :: 3L :: Nil))
    assert(result(2)._1.pivot.contains(7) && result(2)._2.asVector.indicesOfOnes == (7L :: 6L :: Nil))
    assert(result(3)._1.pivot.contains(2) && result(3)._2.asVector.indicesOfOnes == (2L :: 1L :: Nil))
    assert(result(4)._1.pivot.contains(5) && result(4)._2.asVector.indicesOfOnes == (5L :: 4L :: Nil))
    assert(result(5)._1.pivot.contains(8) && result(5)._2.asVector.indicesOfOnes == (8L :: 7L :: Nil))

    // 1-dimensional homology classes die when triangles get filled
    assert(result(6)._1.pivot.contains(15) && result(6)._2.asVector.indicesOfOnes == (15L :: 12L :: 9L :: Nil))
    assert(result(7)._1.pivot.contains(16) && result(7)._2.asVector.indicesOfOnes == (16L :: 13L :: 10L :: Nil))
    assert(result(8)._1.pivot.contains(17) && result(8)._2.asVector.indicesOfOnes == (17L :: 14L :: 11L :: Nil))
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

    val blockColumnRange = Orchestrator.Bounds(
      Orchestrator.BoundStartInclusive(10L),
      Orchestrator.BoundEndInclusive(20L),
    )
    val blockRowRange = Orchestrator.Bounds(
      Orchestrator.BoundStartInclusive(2L),
      Orchestrator.BoundEndInclusive(4L),
    )

    val result = BoundaryMatrixReduction.reduceBlock(columnsInPartition, blockColumnRange, blockRowRange)
      .toList
      .sortBy { case (k, _) => k.indexInMatrix }

    // these were already reduced, because they have index less than left boundary of the current block column range
    assert(result(0) == (Key(4L, Some(2)), Chain(2L :: Nil)))
    assert(result(1) == (Key(8L, Some(3)), Chain(3L :: 2L :: Nil)))

    // this should have been reduced by the algorithm
    assert(result(2) == (Key(17L, Some(4L)), Chain(4L :: 3L :: 2L :: Nil)))

    // this should have been removed as they become zero
    assert(!result.contains((Key(15L, None), Chain(Empty))))
    assert(!result.contains((Key(19L, None), Chain(Empty))))

    // this should not have been reduced, because they have index greater than the current block column range
    assert(result(3) == (Key(24L, Some(3L)), Chain(3L :: 2L :: Nil)))
  }

  "reduceBoundaryMatrix" should
    "reduce boundary matrix of 10 triangles with 3 partitions correctly with DefaultOrchestrator" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)
    val boundaryMatrix = BoundaryMatrix(data)
    val filtrationLength = data.count()
    val orchestrator = new DefaultOrchestrator(3, filtrationLength)

    val result =
      BoundaryMatrixReduction
        .reduceBoundaryMatrix(boundaryMatrix, orchestrator)
        .rdd
        .collect()
        .toList
        .sortBy { case (k, _) => k.indexInMatrix }

    assert(result == nSeparateTrianglesExpectedReducedMatrix(10))
  }

  "reduceBoundaryMatrix" should "reduce boundary matrix of 10 triangles with 8 partitions correctly" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)
    val boundaryMatrix = BoundaryMatrix(data)
    val filtrationLength = data.count()
    val orchestrator = new DefaultOrchestrator(8, filtrationLength)

    val result =
      BoundaryMatrixReduction
        .reduceBoundaryMatrix(boundaryMatrix, orchestrator)
        .rdd
        .collect()
        .toList
        .sortBy { case (k, _) => k.indexInMatrix }

    assert(result == nSeparateTrianglesExpectedReducedMatrix(10))
  }

}
