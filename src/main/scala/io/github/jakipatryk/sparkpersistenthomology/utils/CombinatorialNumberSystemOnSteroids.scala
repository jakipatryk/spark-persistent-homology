package io.github.jakipatryk.sparkpersistenthomology.utils

import scala.collection.Searching.{Found, InsertionPoint}


class CombinatorialNumberSystemOnSteroids(
                                           combinationElementsSetSize: Int,
                                           maxCombinationSize: Int
                                         ) extends Serializable {

  private val combinationsLookup: LocalMatrix[Long] = {
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

  private val combinationsRunningCounts: Array[Long] = {
    val runningCounts = Array.fill(maxCombinationSize + 1)(0L)
    for {
      i <- 1 to maxCombinationSize
    } runningCounts(i) = runningCounts(i - 1) + combinationsLookup(combinationElementsSetSize, i)
    runningCounts
  }

  def getCombinationFromGlobalIndexInLexicographicalOrder(index: Long): Array[Int] = {
    import scala.collection.Searching.SearchImpl

    val correspondingCombinationSize = getCombinationSizeFromGlobalIndexInLexicographicalOrder(index)
    val indexWithinCorrespondingCombinationSizeGroup =
      index - combinationsRunningCounts(correspondingCombinationSize - 1)
    // todo find combination with the greedy algorithm
    val combination = Array.fill(correspondingCombinationSize)(0)
    var currentTarget = indexWithinCorrespondingCombinationSizeGroup

    for (i <- 0 until correspondingCombinationSize) {
      val k = correspondingCombinationSize - i
      val kChooseValues = kthColumnOfCombinationsLookup(k)
      val searchImpl = new SearchImpl[Long, IndexedSeq[Long]](kChooseValues)
      val orderingSkippingZeros: Ordering[Long] = (x: Long, y: Long) =>
        if(x < y) -1
        else if(x == y && y == 0) 1
        else if(x == y) 0
        else 1
      val ithIndex = searchImpl.search(currentTarget)(orderingSkippingZeros) match {
        case Found(foundIndex) => foundIndex
        case InsertionPoint(insertionPoint) => insertionPoint - 1
      }
      combination(i) = ithIndex
      currentTarget -= kChooseValues(ithIndex)
    }

    combination
  }

  def combinationsIterator(startIndex: Long): Iterator[Array[Int]] = new Iterator[Array[Int]] {
    import scala.util.control.Breaks._

    private var currentIndex = startIndex - 1
    private var currentCombination: Option[Array[Int]] = None

    override def hasNext: Boolean = currentIndex + 1 < combinationsRunningCounts.last

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
    // TODO check why the following doesn't compile:
    //    import scala.collection.Searching._
    //    combinationsRunningCounts.search(index)

    import scala.collection.Searching.SearchImpl
    val searchImpl = new SearchImpl[Long, Array[Long]](combinationsRunningCounts)
    searchImpl.search(index) match {
      case Found(foundIndex) => foundIndex + 1
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }

  private def kthColumnOfCombinationsLookup(k: Int): IndexedSeq[Long] = for {
    n <- 0 to combinationElementsSetSize
  } yield combinationsLookup(n, k)

}

object CombinatorialNumberSystemOnSteroids extends App {

  val c = new CombinatorialNumberSystemOnSteroids(10, 4)

  c.combinationsIterator(0).foreach(a => println(a.mkString(", ")))

}
