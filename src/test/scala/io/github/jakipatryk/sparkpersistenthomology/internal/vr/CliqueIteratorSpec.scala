package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.utils.{
  CombinatorialNumberSystem,
  SimplexIndex
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CliqueIteratorSpec extends AnyFlatSpec with Matchers {

  "CliqueIterator" should "iterate over all cliques where startVertex is the strict maximum vertex" in {
    // 4 vertices (0, 1, 2, 3), fully connected graph.
    val neighbors: Array[Array[Int]] = Array(
      Array(3, 2, 1),
      Array(3, 2, 0),
      Array(3, 1, 0),
      Array(2, 1, 0)
    )
    val distances: Array[Array[Float]] = Array.fill(4)(Array.fill(3)(1.0f))
    val distanceMatrix                 = SparseDistanceMatrix(neighbors, distances)
    val cns                            = new CombinatorialNumberSystem(4, 4)
    val combinationSize                = 3

    // startVertex = 3: Should find {3, 2, 1}, {3, 2, 0}, {3, 1, 0}
    val it3  = new CliqueIterator(3, combinationSize, distanceMatrix, cns)
    val res3 = it3.toList.map(idx => cns.getCombinationFromIndex(idx, combinationSize).toList)
    res3 should contain theSameElementsAs List(List(3, 2, 1), List(3, 2, 0), List(3, 1, 0))

    // startVertex = 2: Should find {2, 1, 0}
    val it2  = new CliqueIterator(2, combinationSize, distanceMatrix, cns)
    val res2 = it2.toList.map(idx => cns.getCombinationFromIndex(idx, combinationSize).toList)
    res2 should contain theSameElementsAs List(List(2, 1, 0))

    // startVertex = 1: Should find nothing (no 3-clique has 1 as its maximum vertex)
    val it1 = new CliqueIterator(1, combinationSize, distanceMatrix, cns)
    it1.toList shouldBe empty

    // startVertex = 0: Should find nothing
    val it0 = new CliqueIterator(0, combinationSize, distanceMatrix, cns)
    it0.toList shouldBe empty
  }

  it should "return nothing if no cliques exist of requested size" in {
    // 3 vertices, edges: (0, 1), (0, 2)
    val neighbors: Array[Array[Int]] = Array(
      Array(2, 1), // 0
      Array(0),    // 1
      Array(0)     // 2
    )
    val distances: Array[Array[Float]] = Array(
      Array(1.0f, 1.0f),
      Array(1.0f),
      Array(1.0f)
    )
    val distanceMatrix  = SparseDistanceMatrix(neighbors, distances)
    val cns             = new CombinatorialNumberSystem(3, 3)
    val combinationSize = 3

    val it2 = new CliqueIterator(2, combinationSize, distanceMatrix, cns)
    it2.toList shouldBe empty

    val it1 = new CliqueIterator(1, combinationSize, distanceMatrix, cns)
    it1.toList shouldBe empty

    val it0 = new CliqueIterator(0, combinationSize, distanceMatrix, cns)
    it0.toList shouldBe empty
  }

  it should "handle a linear chain graph without producing any 3-cliques" in {
    // 4 vertices, edges: (0, 1), (1, 2), (2, 3)
    val neighbors: Array[Array[Int]] = Array(
      Array(1),    // 0
      Array(2, 0), // 1
      Array(3, 1), // 2
      Array(2)     // 3
    )
    val distances: Array[Array[Float]] = Array(
      Array(1.0f),
      Array(1.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(1.0f)
    )
    val distanceMatrix  = SparseDistanceMatrix(neighbors, distances)
    val cns             = new CombinatorialNumberSystem(4, 4)
    val combinationSize = 3

    for (v <- 0 until neighbors.length) {
      val iterator = new CliqueIterator(v, combinationSize, distanceMatrix, cns)
      iterator.toList shouldBe empty
    }
  }

  it should "work correctly for 1-clique (vertices)" in {
    // 3 isolated vertices.
    val neighbors: Array[Array[Int]] = Array(
      Array(),
      Array(),
      Array()
    )
    val distances: Array[Array[Float]] = Array(
      Array(),
      Array(),
      Array()
    )
    val distanceMatrix  = SparseDistanceMatrix(neighbors, distances)
    val cns             = new CombinatorialNumberSystem(3, 3)
    val combinationSize = 1

    for (v <- 0 until 3) {
      val it = new CliqueIterator(v, combinationSize, distanceMatrix, cns)
      it.toList.map(idx => cns.getCombinationFromIndex(idx, combinationSize).toList) shouldBe List(
        List(v)
      )
    }
  }

  it should "return nothing for combination size larger than available neighbors" in {
    // Graph: 0-1, 0-2, 1-2.
    val neighbors = Array(
      Array(2, 1),
      Array(2, 0),
      Array(1, 0)
    )
    val distances       = Array.fill(3)(Array.fill(2)(1.0f))
    val distanceMatrix  = SparseDistanceMatrix(neighbors, distances)
    val cns             = new CombinatorialNumberSystem(3, 3)
    val combinationSize = 4 // impossible

    for (v <- 0 until 3) {
      val it = new CliqueIterator(v, combinationSize, distanceMatrix, cns)
      it.toList shouldBe empty
    }
  }

  it should "work correctly for disjoint components" in {
    // Component 1: {0, 1}, Component 2: {2, 3}
    val neighbors = Array(
      Array(1), // 0
      Array(0), // 1
      Array(3), // 2
      Array(2)  // 3
    )
    val distances       = Array.fill(4)(Array.fill(1)(1.0f))
    val distanceMatrix  = SparseDistanceMatrix(neighbors, distances)
    val cns             = new CombinatorialNumberSystem(4, 4)
    val combinationSize = 2

    // startVertex=1 finds {1,0}. startVertex=3 finds {3,2}. startVertex=0 and 2 should find nothing (as startVertex)
    val it1 = new CliqueIterator(1, combinationSize, distanceMatrix, cns)
    it1.toList.map(idx => cns.getCombinationFromIndex(idx, combinationSize).toList) shouldBe List(
      List(1, 0)
    )

    val it3 = new CliqueIterator(3, combinationSize, distanceMatrix, cns)
    it3.toList.map(idx => cns.getCombinationFromIndex(idx, combinationSize).toList) shouldBe List(
      List(3, 2)
    )

    new CliqueIterator(0, combinationSize, distanceMatrix, cns).toList shouldBe empty
    new CliqueIterator(2, combinationSize, distanceMatrix, cns).toList shouldBe empty
  }
}
