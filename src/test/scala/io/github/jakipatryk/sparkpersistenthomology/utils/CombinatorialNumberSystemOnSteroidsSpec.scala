package io.github.jakipatryk.sparkpersistenthomology.utils

import org.scalatest.flatspec.AnyFlatSpec

class CombinatorialNumberSystemOnSteroidsSpec extends AnyFlatSpec {

  behavior of "getCombinationFromGlobalIndexInLexicographicalOrder"

  it should "return good combination for 1-combination index" in {
    val systemUpTo1 = CombinatorialNumberSystemOnSteroids(8, 1)

    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(0) === Array(0))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(1) === Array(1))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(2) === Array(2))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(3) === Array(3))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(4) === Array(4))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(5) === Array(5))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(6) === Array(6))
    assert(systemUpTo1.getCombinationFromGlobalIndexInLexicographicalOrder(7) === Array(7))

    val systemUpTo3 = CombinatorialNumberSystemOnSteroids(8, 3)

    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(0) === Array(0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(1) === Array(1))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(2) === Array(2))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(3) === Array(3))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(4) === Array(4))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(5) === Array(5))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(6) === Array(6))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(7) === Array(7))

  }

  it should "return good combination for 2-combination index" in {
    val systemUpTo3 = CombinatorialNumberSystemOnSteroids(8, 3)

    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(8) === Array(1, 0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(9) === Array(2, 0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(10) === Array(2, 1))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(35) === Array(7, 6))
  }

  it should "return good combination for 3-combination index" in {
    val systemUpTo3 = CombinatorialNumberSystemOnSteroids(8, 3)

    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(36) === Array(2, 1, 0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(37) === Array(3, 1, 0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(38) === Array(3, 2, 0))
    assert(systemUpTo3.getCombinationFromGlobalIndexInLexicographicalOrder(91) === Array(7, 6, 5))
  }

  behavior of "combinationsIterator(startIndex = 0)"

  it should "return a correct iterator for system with `maxCombinationSize` = 1" in {
    val systemUpTo1 = CombinatorialNumberSystemOnSteroids(8, 1)

    val actualIterator = systemUpTo1.combinationsIterator(0)
    val expectedIterator = Iterator(0, 1, 2, 3, 4, 5, 6, 7).map(Array(_))

    (actualIterator zip expectedIterator) foreach { case (actual, expected) =>
      assert(actual === expected)
    }
  }

  it should "return a correct iterator for system with `maxCombinationSize` = 2" in {
    val systemUpTo2 = CombinatorialNumberSystemOnSteroids(8, 2)

    val actualIterator = systemUpTo2.combinationsIterator(0)
    val expectedIterator = Iterator(
      Array(0),
      Array(1),
      Array(2),
      Array(3),
      Array(4),
      Array(5),
      Array(6),
      Array(7),
      Array(1, 0),
      Array(2, 0),
      Array(2, 1),
      Array(3, 0),
      Array(3, 1),
      Array(3, 2),
      Array(4, 0),
      Array(4, 1),
      Array(4, 2),
      Array(4, 3),
      Array(5, 0),
      Array(5, 1),
      Array(5, 2),
      Array(5, 3),
      Array(5, 4),
      Array(6, 0),
      Array(6, 1),
      Array(6, 2),
      Array(6, 3),
      Array(6, 4),
      Array(6, 5),
      Array(7, 0),
      Array(7, 1),
      Array(7, 2),
      Array(7, 3),
      Array(7, 4),
      Array(7, 5),
      Array(7, 6),
    )

    (actualIterator zip expectedIterator) foreach { case (actual, expected) =>
      assert(actual === expected)
    }
  }

  behavior of "combinationsIterator(startIndex > 0)"

  it should "return a correct iterator for system with `maxCombinationSize` = 1" in {
    val systemUpTo1 = CombinatorialNumberSystemOnSteroids(8, 1)

    val actualIterator = systemUpTo1.combinationsIterator(5)
    val expectedIterator = Iterator(5, 6, 7).map(Array(_))

    (actualIterator zip expectedIterator) foreach { case (actual, expected) =>
      assert(actual === expected)
    }
  }

  it should "return a correct iterator for system with `maxCombinationSize` = 2" in {
    val systemUpTo2 = CombinatorialNumberSystemOnSteroids(8, 2)

    val actualIterator = systemUpTo2.combinationsIterator(25)
    val expectedIterator = Iterator(
      Array(6, 2),
      Array(6, 3),
      Array(6, 4),
      Array(6, 5),
      Array(7, 0),
      Array(7, 1),
      Array(7, 2),
      Array(7, 3),
      Array(7, 4),
      Array(7, 5),
      Array(7, 6),
    )

    (actualIterator zip expectedIterator) foreach { case (actual, expected) =>
      assert(actual === expected)
    }
  }

}
