package io.github.jakipatryk.sparkpersistenthomology.utils

/**
 * Implementation of concatenation of Combinatorial Number Systems up to `maxCombinationSize`.
 *
 * @param combinationElementsSetSize Number of elements of the (ordered) multiset for which combinations are produced.
 * @param maxCombinationSize Concatenate CNSs up to this, so included are sizes: 1, 2, ..., `maxCombinationSize`
 */
class CombinatorialNumberSystemOnSteroids(
                                           combinationElementsSetSize: Int,
                                           maxCombinationSize: Int
                                         ) extends Serializable {

  private[sparkpersistenthomology] val combinationsLookup: LocalMatrix[Long] = {
    val matrix = LocalMatrix.zero[Long](combinationElementsSetSize + 1, maxCombinationSize + 1)

    for(i <- 0 to combinationElementsSetSize) {
      matrix(i, 0) = 1
      if(i <= maxCombinationSize) matrix(i, i) = 1
    }

    for {
      i <- 2 to combinationElementsSetSize
      j <- 1 until i
      if j <= maxCombinationSize
    } matrix(i, j) = matrix(i - 1, j - 1) + matrix(i - 1, j)

    matrix
  }

  private[sparkpersistenthomology] val combinationsRunningCounts: Array[Long] = {
    val runningCounts = Array.fill(maxCombinationSize + 1)(0L)
    for {
      i <- 1 to maxCombinationSize
    } runningCounts(i) = runningCounts(i - 1) + combinationsLookup(combinationElementsSetSize, i)
    runningCounts
  }

  /**
   * A count of all combinations of `combinationElementsSetSize` elements up to `maxCombinationSize`-combination.
   *
   * In other words, if we let n := combinationElementsSetSize and k := maxCombinationSize:
   * (n choose 1) + (n choose 2) + ... + (n choose k)
   */
  def allCombinationsCount: Long = combinationsRunningCounts.last

  /**
   * Computes combination (actually indices of elements in the combination)
   * for an index in (global, meaning that in concatenated and limited to `maxCombinationSize`)
   * lexicographical order of combinations.
   *
   * For `maxCombinationSize = 1` lexicographical order of combinations is trivial:
   * (0), (1), ..., (combinationElementsSetSize)
   *
   * Then for `maxCombinationSize = 2` it is:
   * (0), (1), ..., (combinationElementsSetSize),
   * (1, 0), (2, 0), (2, 1), (3, 0), ..., (combinationElementsSetSize, combinationElementsSetSize - 1)
   *
   * And so on.
   *
   * @example
   * val system = new CombinatorialNumberSystemOnSteroids(10, 2)
   * system.getCombinationFromGlobalIndexInLexicographicalOrder(0) //  (0)
   * system.getCombinationFromGlobalIndexInLexicographicalOrder(5) //  (5)
   * system.getCombinationFromGlobalIndexInLexicographicalOrder(10) // (1, 0)
   */
  def getCombinationFromGlobalIndexInLexicographicalOrder(index: Long): Array[Int] = {
    import scala.collection.Searching._

    val correspondingCombinationSize = getCombinationSizeFromGlobalIndexInLexicographicalOrder(index)
    val indexWithinCorrespondingCombinationSizeGroup =
      index - combinationsRunningCounts(correspondingCombinationSize - 1)

    val combination = Array.fill(correspondingCombinationSize)(0)
    var currentTarget = indexWithinCorrespondingCombinationSizeGroup

    for (i <- 0 until correspondingCombinationSize) {
      val k = correspondingCombinationSize - i
      val kChooseValues = kthColumnOfCombinationsLookup(k)
      val orderingSkippingZeros: Ordering[Long] = (x: Long, y: Long) =>
        if(x < y) -1
        else if(x == y && y == 0) 1
        else if(x == y) 0
        else 1
      val ithIndex = kChooseValues.search(currentTarget)(orderingSkippingZeros) match {
        case Found(foundIndex) => foundIndex
        case InsertionPoint(insertionPoint) => insertionPoint - 1
      }
      combination(i) = ithIndex
      currentTarget -= kChooseValues(ithIndex)
    }

    combination
  }

  /**
   * Returns an iterator of combinations in global lexicographical order starting with one with index `startIndex`.
   */
  def combinationsIterator(startIndex: Long): Iterator[Array[Int]] = new Iterator[Array[Int]] {
    import scala.util.control.Breaks._

    private var currentIndex = startIndex - 1
    private var currentCombination: Option[Array[Int]] = None

    override def hasNext: Boolean = currentIndex + 1 < allCombinationsCount

    override def next(): Array[Int] = {
      currentIndex += 1
      currentCombination = currentCombination match {
        case None =>
          Some(getCombinationFromGlobalIndexInLexicographicalOrder(currentIndex))
        case Some(combination) =>
          val size = combination.length
          val nextCombination = {
            if (currentIndex == combinationsRunningCounts(size)) {
              val arr = Array.fill(size + 1)(0)
              for (i <- 0 until (size + 1)) arr(i) = size - i
              arr
            }
            else {
              breakable {
                var overflow = false
                for (i <- (size - 1) until 0 by (-1)) {
                  if (overflow && combination(i) < combination(i - 1) - 1) {
                    combination(i) += 1
                    break
                  }
                  else if (overflow) {
                    combination(i) = size - 1 - i
                    overflow = true
                  }
                  else if (combination(i) < combination(i - 1) - 1) {
                    combination(i) += 1
                    break
                  } else {
                    combination(i) = size - 1 - i
                    overflow = true
                  }
                }
                combination(0) += 1
              }
              combination
            }
          }
          Some(nextCombination)
      }
      currentCombination.get
    }
  }

  private def getCombinationSizeFromGlobalIndexInLexicographicalOrder(index: Long): Int = {
    import scala.collection.Searching._

    combinationsRunningCounts.search(index) match {
      case Found(foundIndex) => foundIndex + 1
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }

  private def kthColumnOfCombinationsLookup(k: Int): IndexedSeq[Long] = for {
    n <- 0 to combinationElementsSetSize
  } yield combinationsLookup(n, k)

}

object CombinatorialNumberSystemOnSteroids {

  def apply(combinationElementsSetSize: Int, maxCombinationSize: Int): CombinatorialNumberSystemOnSteroids =
    new CombinatorialNumberSystemOnSteroids(combinationElementsSetSize, maxCombinationSize)

}
