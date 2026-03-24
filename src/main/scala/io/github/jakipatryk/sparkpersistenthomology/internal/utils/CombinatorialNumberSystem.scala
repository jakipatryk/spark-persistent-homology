package io.github.jakipatryk.sparkpersistenthomology.internal.utils

/** Implementation of Combinatorial Number System for a single `combinationSize`.
  *
  * @param combinationElementsSetSize
  *   Number of elements of the (ordered) multiset for which combinations are produced.
  * @param combinationSize
  *   The size of the combinations produced.
  */
private[sparkpersistenthomology] class CombinatorialNumberSystem(
  combinationElementsSetSize: Int,
  val combinationSize: Int
) extends Serializable {

  private[sparkpersistenthomology] val combinationsLookup: LocalMatrix[Long] = {
    val matrix = LocalMatrix.zero[Long](combinationElementsSetSize + 1, combinationSize + 1)

    for (i <- 0 to combinationElementsSetSize) {
      matrix(i, 0) = 1
      if (i <= combinationSize) matrix(i, i) = 1
    }

    for {
      i <- 2 to combinationElementsSetSize
      j <- 1 until i
      if j <= combinationSize
    } matrix(i, j) = matrix(i - 1, j - 1) + matrix(i - 1, j)

    matrix
  }

  /** A count of all combinations of `combinationElementsSetSize` elements of size
    * `combinationSize`.
    */
  def allCombinationsCount: Long = combinationsLookup(combinationElementsSetSize, combinationSize)

  /** Computes combination (actually indices of elements in the combination) for an index in
    * lexicographical order of combinations.
    *
    * @example
    *   val system = new CombinatorialNumberSystemOnSteroids(10, 2)
    *   system.getCombinationFromIndex(0) // (1, 0) system.getCombinationFromIndex(1) // (2, 0)
    *   system.getCombinationFromIndex(2) // (2, 1)
    */
  def getCombinationFromIndex(index: Long): Array[Int] = {
    if (index < 0 || index >= allCombinationsCount) {
      throw new IndexOutOfBoundsException(
        s"Index $index is out of range [0, $allCombinationsCount)"
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
  def getIndexFromCombination(combination: Array[Int]): Long = {
    var index = 0L
    var i     = 0
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
  def combinationsIterator(startIndex: Long): Iterator[Array[Int]] = new Iterator[Array[Int]] {
    private var currentIndex                           = startIndex - 1
    private var currentCombination: Option[Array[Int]] = None

    override def hasNext: Boolean = currentIndex + 1 < allCombinationsCount

    override def next(): Array[Int] = {
      currentIndex += 1
      currentCombination = currentCombination match {
        case None =>
          Some(getCombinationFromIndex(currentIndex))
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

}

private[sparkpersistenthomology] object CombinatorialNumberSystem {

  def apply(
    combinationElementsSetSize: Int,
    combinationSize: Int
  ): CombinatorialNumberSystem =
    new CombinatorialNumberSystem(combinationElementsSetSize, combinationSize)

}
