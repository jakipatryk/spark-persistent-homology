package io.github.jakipatryk.sparkpersistenthomology.internal.utils

import org.scalatest.flatspec.AnyFlatSpec

class CombinatorialNumberSystemOnSteroidsSpec extends AnyFlatSpec {

  behavior of "getCombinationFromIndex"

  behavior of "getIndexFromCombination"

  it should "return correct index for 1-combination" in {
    val system1 = CombinatorialNumberSystem(8, 3)

    assert(system1.getIndexFromCombination(Array(0)) === 0)
    assert(system1.getIndexFromCombination(Array(1)) === 1)
    assert(system1.getIndexFromCombination(Array(2)) === 2)
    assert(system1.getIndexFromCombination(Array(3)) === 3)
    assert(system1.getIndexFromCombination(Array(4)) === 4)
    assert(system1.getIndexFromCombination(Array(5)) === 5)
    assert(system1.getIndexFromCombination(Array(6)) === 6)
    assert(system1.getIndexFromCombination(Array(7)) === 7)
  }

  it should "return correct index for 2-combination" in {
    val system2 = CombinatorialNumberSystem(8, 3)

    assert(system2.getIndexFromCombination(Array(1, 0)) === 0)
    assert(system2.getIndexFromCombination(Array(2, 0)) === 1)
    assert(system2.getIndexFromCombination(Array(2, 1)) === 2)
    assert(system2.getIndexFromCombination(Array(7, 6)) === 27)
  }

  it should "return correct index for 3-combination" in {
    val system3 = CombinatorialNumberSystem(8, 3)

    assert(system3.getIndexFromCombination(Array(2, 1, 0)) === 0)
    assert(system3.getIndexFromCombination(Array(3, 1, 0)) === 1)
    assert(system3.getIndexFromCombination(Array(3, 2, 0)) === 2)
    assert(system3.getIndexFromCombination(Array(7, 6, 5)) === 55)
  }

  behavior of "getCombinationFromIndex"

  it should "return good combination for 1-combination index" in {
    val system1 = CombinatorialNumberSystem(8, 3)

    assert(system1.getCombinationFromIndex(0, 1) === Array(0))
    assert(system1.getCombinationFromIndex(1, 1) === Array(1))
    assert(system1.getCombinationFromIndex(2, 1) === Array(2))
    assert(system1.getCombinationFromIndex(3, 1) === Array(3))
    assert(system1.getCombinationFromIndex(4, 1) === Array(4))
    assert(system1.getCombinationFromIndex(5, 1) === Array(5))
    assert(system1.getCombinationFromIndex(6, 1) === Array(6))
    assert(system1.getCombinationFromIndex(7, 1) === Array(7))
  }

  it should "return good combination for 2-combination index" in {
    val system2 = CombinatorialNumberSystem(8, 3)

    assert(system2.getCombinationFromIndex(0, 2) === Array(1, 0))
    assert(system2.getCombinationFromIndex(1, 2) === Array(2, 0))
    assert(system2.getCombinationFromIndex(2, 2) === Array(2, 1))
    assert(system2.getCombinationFromIndex(27, 2) === Array(7, 6))
  }

  it should "return good combination for 3-combination index" in {
    val system3 = CombinatorialNumberSystem(8, 3)

    assert(system3.getCombinationFromIndex(0, 3) === Array(2, 1, 0))
    assert(system3.getCombinationFromIndex(1, 3) === Array(3, 1, 0))
    assert(system3.getCombinationFromIndex(2, 3) === Array(3, 2, 0))
    assert(system3.getCombinationFromIndex(55, 3) === Array(7, 6, 5))
  }

  it should "throw when index is negative" in {
    val system = CombinatorialNumberSystem(8, 3)

    assertThrows[IndexOutOfBoundsException] {
      system.getCombinationFromIndex(-1, 3)
    }
  }

  it should "throw when index is >= allCombinationsCount" in {
    val system = CombinatorialNumberSystem(8, 3)
    val count  = system.allCombinationsCount(3)

    assertThrows[IndexOutOfBoundsException] {
      system.getCombinationFromIndex(count, 3)
    }
    assertThrows[IndexOutOfBoundsException] {
      system.getCombinationFromIndex(count + 1, 3)
    }
  }

  behavior of "combinationsIterator(startIndex = 0)"

  it should "return a correct iterator for system with `combinationSize` = 1" in {
    val system1 = CombinatorialNumberSystem(8, 3)

    val actualIterator   = system1.combinationsIterator(0, 1)
    val expectedIterator = Iterator(0, 1, 2, 3, 4, 5, 6, 7).map(Array(_))

    assert(actualIterator.map(_.toList).toList === expectedIterator.map(_.toList).toList)
  }

  it should "return a correct iterator for system with `combinationSize` = 2" in {
    val system2 = CombinatorialNumberSystem(8, 3)

    val actualIterator = system2.combinationsIterator(0, 2)
    val expectedIterator = Iterator(
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
      Array(7, 6)
    )

    assert(actualIterator.map(_.toList).toList === expectedIterator.map(_.toList).toList)
  }

  behavior of "combinationsIterator(startIndex > 0)"

  it should "return a correct iterator for system with `combinationSize` = 1" in {
    val system1 = CombinatorialNumberSystem(8, 3)

    val actualIterator   = system1.combinationsIterator(5, 1)
    val expectedIterator = Iterator(5, 6, 7).map(Array(_))

    assert(actualIterator.map(_.toList).toList === expectedIterator.map(_.toList).toList)
  }

  it should "return a correct iterator for system with `combinationSize` = 2" in {
    val system2 = CombinatorialNumberSystem(8, 3)

    val actualIterator = system2.combinationsIterator(17, 2)
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
      Array(7, 6)
    )

    assert(actualIterator.map(_.toList).toList === expectedIterator.map(_.toList).toList)
  }

  behavior of "subcombinationsIterator"

  it should "return correct subcombinations for 3-combination" in {
    val system      = CombinatorialNumberSystem(8, 3)
    val combination = Array(3, 2, 0)
    val expected = List(
      (1L, Array(2, 0), 3),
      (3L, Array(3, 0), 2),
      (5L, Array(3, 2), 0)
    )

    val actual = system
      .subcombinationsIterator(combination)
      .map { case (idx, arr, removedElement) =>
        (idx, arr.toList, removedElement)
      }
      .toList

    val expectedList = expected.map { case (idx, arr, removedElement) =>
      (idx, arr.toList, removedElement)
    }
    assert(actual === expectedList)
  }

  it should "return correct subcombinations for 2-combination" in {
    val system      = CombinatorialNumberSystem(8, 3)
    val combination = Array(4, 2)
    val expected = List(
      (2L, Array(2), 4),
      (4L, Array(4), 2)
    )

    val actual = system
      .subcombinationsIterator(combination)
      .map { case (idx, arr, removedElement) =>
        (idx, arr.toList, removedElement)
      }
      .toList

    val expectedList = expected.map { case (idx, arr, removedElement) =>
      (idx, arr.toList, removedElement)
    }
    assert(actual === expectedList)
  }

  behavior of "supcombinationsIterator"

  it should "return correct supcombinations for 2-combination" in {
    val system      = CombinatorialNumberSystem(8, 3)
    val combination = Array(2, 0) // index 1
    val expected = List(
      (36L, Array(7, 2, 0), 7),
      (21L, Array(6, 2, 0), 6),
      (11L, Array(5, 2, 0), 5),
      (5L, Array(4, 2, 0), 4),
      (2L, Array(3, 2, 0), 3),
      (0L, Array(2, 1, 0), 1)
    )

    val actual = system
      .supcombinationsIterator(combination)
      .map { case (idx, arr, addedElement) =>
        (idx, arr.toList, addedElement)
      }
      .toList

    val expectedList = expected.map { case (idx, arr, addedElement) =>
      (idx, arr.toList, addedElement)
    }
    assert(actual === expectedList)
  }

  it should "return correct supcombinations for 1-combination" in {
    val system      = CombinatorialNumberSystem(8, 3)
    val combination = Array(2)
    val expected = List(
      (23L, Array(7, 2), 7),
      (17L, Array(6, 2), 6),
      (12L, Array(5, 2), 5),
      (8L, Array(4, 2), 4),
      (5L, Array(3, 2), 3),
      (2L, Array(2, 1), 1),
      (1L, Array(2, 0), 0)
    )

    val actual = system
      .supcombinationsIterator(combination)
      .map { case (idx, arr, addedElement) =>
        (idx, arr.toList, addedElement)
      }
      .toList

    val expectedList = expected.map { case (idx, arr, addedElement) =>
      (idx, arr.toList, addedElement)
    }
    assert(actual === expectedList)
  }

}
