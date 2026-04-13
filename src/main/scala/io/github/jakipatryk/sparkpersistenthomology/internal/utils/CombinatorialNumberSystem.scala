package io.github.jakipatryk.sparkpersistenthomology.internal.utils

/** Implementation of Combinatorial Number System for `combinationSize` up to `maxCombinationSize`.
  *
  * @param combinationElementsSetSize
  *   Number of elements of the (ordered) multiset for which combinations are produced.
  * @param maxCombinationSize
  *   The maximum size of the combinations produced.
  */
private[sparkpersistenthomology] class CombinatorialNumberSystem(
  val combinationElementsSetSize: Int,
  val maxCombinationSize: Int
) extends Serializable {

  import CombinatorialNumberSystem._

  private[sparkpersistenthomology] val combinationsLookup: LocalMatrix[Long] = {
    val matrix = LocalMatrix.zero[Long](combinationElementsSetSize + 1, maxCombinationSize + 1)

    for (i <- 0 to combinationElementsSetSize) {
      matrix(i, 0) = 1
      if (i <= maxCombinationSize) matrix(i, i) = 1
    }

    for {
      i <- 2 to combinationElementsSetSize
      j <- 1 until i
      if j <= maxCombinationSize
    } matrix(i, j) = matrix(i - 1, j - 1) + matrix(i - 1, j)

    matrix
  }

  /** A count of all combinations of `combinationElementsSetSize` elements of size
    * `combinationSize`.
    */
  def allCombinationsCount(combinationSize: Int): Long = {
    combinationsLookup(combinationElementsSetSize, combinationSize)
  }

  /** Computes combination (actually indices of elements in the combination) for an index in
    * lexicographical order of combinations.
    *
    * @example
    *   val system = new CombinatorialNumberSystemOnSteroids(10, 2)
    *   system.getCombinationFromIndex(0, 2) // (1, 0)
    */
  def getCombinationFromIndex(index: Index, combinationSize: Int): Combination = {
    val count = allCombinationsCount(combinationSize)
    if (index < 0 || index >= count) {
      throw new IndexOutOfBoundsException(
        s"Index $index is out of range [0, $count)"
      )
    }
    val combination   = Array.fill(combinationSize)(0)
    var currentTarget = index

    for (i <- 0 until combinationSize) {
      val k = combinationSize - i

      var low      = 0
      var high     = combinationElementsSetSize
      var ithIndex = low

      while (low <= high) {
        val mid    = (low + high) >>> 1
        val midVal = combinationsLookup(mid, k)
        if (midVal <= currentTarget) {
          ithIndex = mid
          low = mid + 1
        } else {
          high = mid - 1
        }
      }

      combination(i) = ithIndex
      currentTarget -= combinationsLookup(ithIndex, k)
    }

    combination
  }

  /** Computes the index in lexicographical order for a given combination. Combination must be
    * sorted in descending order.
    */
  def getIndexFromCombination(combination: Combination): Index = {
    val combinationSize = combination.length
    var index           = 0L
    var i               = 0
    while (i < combinationSize) {
      val k = combinationSize - i
      index += combinationsLookup(combination(i), k)
      i += 1
    }
    index
  }

  /** Returns an iterator of combinations in lexicographical order starting with one with index
    * `startIndex`.
    */
  def combinationsIterator(startIndex: Index, combinationSize: Int): Iterator[Combination] =
    new Iterator[Array[Int]] {
      private val count                                  = allCombinationsCount(combinationSize)
      private var currentIndex                           = startIndex - 1
      private var currentCombination: Option[Array[Int]] = None

      override def hasNext: Boolean = currentIndex + 1 < count

      override def next(): Array[Int] = {
        currentIndex += 1
        currentCombination = currentCombination match {
          case None =>
            Some(getCombinationFromIndex(currentIndex, combinationSize))
          case Some(combination) =>
            var i     = combinationSize - 1
            var broke = false
            while (i > 0 && !broke) {
              if (combination(i) < combination(i - 1) - 1) {
                combination(i) += 1
                broke = true
              } else {
                combination(i) = combinationSize - 1 - i
                i -= 1
              }
            }
            if (!broke && combinationSize > 0) {
              combination(0) += 1
            }
            Some(combination)
        }
        currentCombination.get
      }
    }

  /** Returns an iterator with all subcombinations (of length of input combination -1) of a given
    * combination.
    *
    * The elements are returned in ascending order in order of the Combinatorial Number System.
    *
    * Tuple in the iterator represents the following:
    *   - `Index` - index of the subcombination in CNS
    *   - `Int` - the index of the element that was removed from the input combination
    *   - `Int` - the element that was removed
    */
  def subcombinationsIndicesIterator(combination: Combination): Iterator[(Index, Int, Int)] = {
    val n = combination.length

    new Iterator[(Index, Int, Int)] {
      private var i = 0

      override def hasNext: Boolean = i < n

      override def next(): (Index, Int, Int) = {
        // Calculate index of sub-combination [a_n, ..., a_{i+1}, a_{i-1}, ..., a_1]
        // Elements before i (original positions n...n-i+1) are now at positions n-1...n-i
        // Elements after i (original positions n-i-1...1) are now at positions n-i-1...1
        var index = 0L
        var j     = 0
        while (j < i) {
          index += combinationsLookup(combination(j), n - 1 - j)
          j += 1
        }
        j = i + 1
        while (j < n) {
          index += combinationsLookup(combination(j), n - j)
          j += 1
        }

        val removedIndex   = i
        val removedElement = combination(i)
        i += 1

        (index, removedIndex, removedElement)
      }
    }
  }

  /** Returns an iterator with all supcombinations (of length of input combination +1) of a given
    * combination.
    *
    * The elements are returned in descending order in order of the Combinatiorial Number System.
    *
    * Tuple in the iterator represents the following:
    *   - `Index` - index of the supcombination in CNS
    *   - `Int` - the element that was added
    */
  def supcombinationsIndicesIterator(combination: Combination): Iterator[(Index, Int)] = {
    val n = combination.length

    new Iterator[(Index, Int)] {
      private var currentElementToAdd = combinationElementsSetSize - 1
      private var combinationIndex    = 0

      // Pre-calculate the base index parts that don't change frequently.
      // Index = sum_{i=0}^{n} combinationsLookup(sup(i), (n+1)-i)
      private var currentFullIndex = 0L

      private def advance(): Unit = {
        while (
          currentElementToAdd >= 0 &&
          combinationIndex < n &&
          combination(combinationIndex) == currentElementToAdd
        ) {
          currentElementToAdd -= 1
          combinationIndex += 1
        }
      }

      advance()

      override def hasNext: Boolean = currentElementToAdd >= 0

      override def next(): (Index, Int) = {
        val addedElement = currentElementToAdd

        // Calculate index incrementally or fully.
        // For simplicity and correctness first, let's do it efficiently but robustly.
        // We can optimize this if we see it's still slow.
        var index = 0L
        var i     = 0
        while (i < combinationIndex) {
          index += combinationsLookup(combination(i), n + 1 - i)
          i += 1
        }
        index += combinationsLookup(addedElement, n + 1 - combinationIndex)
        i = combinationIndex
        while (i < n) {
          index += combinationsLookup(combination(i), n - i)
          i += 1
        }

        currentElementToAdd -= 1
        advance()

        (index, addedElement)
      }
    }
  }

}

private[sparkpersistenthomology] object CombinatorialNumberSystem {

  type Index       = Long
  type Combination = Array[Int]

  def apply(
    combinationElementsSetSize: Int,
    maxCombinationSize: Int
  ): CombinatorialNumberSystem =
    new CombinatorialNumberSystem(combinationElementsSetSize, maxCombinationSize)

}
