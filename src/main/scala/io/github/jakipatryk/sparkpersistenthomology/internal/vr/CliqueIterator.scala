package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

/** Iterator that finds all `combinationSize`-cliques (simplices) in a graph that contain a specific
  * vertex `startVertex`.
  *
  * It generates combinations strictly in descending order (e.g., `(v_0, v_1, ..., v_k)` where `v_0
  * > v_1 > ... > v_k`) and returns their indices in the Combinatorial Number System.
  *
  * To avoid duplicate generation of the same clique by different starting vertices, it enforces
  * that `startVertex` is strictly the maximum vertex in the clique. It does this by skipping all
  * neighbors of `startVertex` that are larger than `startVertex`.
  *
  * It uses a depth-first search approach, avoiding allocations during iteration by reusing
  * pre-allocated arrays.
  */
private[vr] class CliqueIterator(
  startVertex: Int,
  combinationSize: Int,
  distanceMatrix: SparseDistanceMatrix,
  combinatorialNumberSystem: CombinatorialNumberSystem
) extends Iterator[BigInt] {
  private val degree               = distanceMatrix.neighbors(startVertex).length
  private val currentCombination   = new Array[Int](combinationSize)
  private val candidatesStack      = Array.fill(combinationSize)(new Array[Int](degree))
  private val candidatesCountStack = new Array[Int](combinationSize)
  private val candidateIndexStack  = new Array[Int](combinationSize)
  private var depth                = 1

  private var nextIndex: BigInt = BigInt(-1)

  initializeSearchState(startVertex)
  if (combinationSize > 1) {
    advance()
  } else {
    nextIndex = combinatorialNumberSystem.getIndexFromCombination(currentCombination)
  }

  private def initializeSearchState(initialVertex: Int): Unit = {
    currentCombination(0) = initialVertex

    // Only need to initialize candidates stack if we are looking for cliques of size > 1
    if (combinationSize > 1) {
      val neighbors     = distanceMatrix.neighbors(initialVertex)
      var neighborIndex = 0

      // Skip neighbors that are larger than startVertex to ensure startVertex is the maximum vertex in the clique
      while (neighborIndex < neighbors.length && neighbors(neighborIndex) > initialVertex) {
        neighborIndex += 1
      }

      var validNeighborCount = 0
      while (neighborIndex < neighbors.length) {
        candidatesStack(1)(validNeighborCount) = neighbors(neighborIndex)
        validNeighborCount += 1
        neighborIndex += 1
      }

      candidatesCountStack(1) = validNeighborCount
      candidateIndexStack(1) = 0
    }
  }

  private def advance(): Unit = {
    nextIndex = BigInt(-1)

    // Iterative Depth-First Search
    while (depth > 0 && nextIndex == BigInt(-1)) {
      if (candidateIndexStack(depth) < candidatesCountStack(depth)) {
        val currentVertex = candidatesStack(depth)(candidateIndexStack(depth))
        currentCombination(depth) = currentVertex

        if (depth + 1 < combinationSize) {
          // Need to go deeper
          val currentVertexNeighbors = distanceMatrix.neighbors(currentVertex)
          val nextCandidates         = candidatesStack(depth + 1)

          val currentCandidates      = candidatesStack(depth)
          val currentCandidatesCount = candidatesCountStack(depth)

          var candidatePointer    = candidateIndexStack(depth) + 1
          var neighborPointer     = 0
          var nextCandidatesCount = 0

          while (
            candidatePointer < currentCandidatesCount && neighborPointer < currentVertexNeighbors.length
          ) {
            val candidate = currentCandidates(candidatePointer)
            val neighbor  = currentVertexNeighbors(neighborPointer)

            if (candidate == neighbor) {
              nextCandidates(nextCandidatesCount) = candidate
              nextCandidatesCount += 1
              candidatePointer += 1
              neighborPointer += 1
            } else if (candidate > neighbor) {
              candidatePointer += 1
            } else {
              neighborPointer += 1
            }
          }

          candidateIndexStack(depth) += 1

          candidatesCountStack(depth + 1) = nextCandidatesCount
          candidateIndexStack(depth + 1) = 0
          depth += 1
        } else {
          // Clique of requested size found
          nextIndex = combinatorialNumberSystem.getIndexFromCombination(currentCombination)
          candidateIndexStack(depth) += 1
        }
      } else {
        // Exhausted all candidates at current depth, backtrack
        depth -= 1
      }
    }
  }

  override def hasNext: Boolean = nextIndex != BigInt(-1)

  override def next(): BigInt = {
    if (!hasNext) throw new java.util.NoSuchElementException()
    val res = nextIndex
    if (combinationSize > 1) {
      advance()
    } else {
      nextIndex = BigInt(-1)
    }
    res
  }
}
