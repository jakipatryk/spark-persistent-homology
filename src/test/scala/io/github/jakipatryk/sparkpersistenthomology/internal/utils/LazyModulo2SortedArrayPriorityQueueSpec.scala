package io.github.jakipatryk.sparkpersistenthomology.internal.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen
import scala.collection.mutable

class LazyModulo2SortedArrayPriorityQueueSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks {

  behavior of "LazyModulo2SortedArrayPriorityQueue"

  it should "return elements in descending order and apply modulo 2 adjustment" in {
    val pq = new LazyModulo2SortedArrayPriorityQueue[Int]()

    pq += Array(10, 8, 5, 2)
    pq += Array(8, 6, 2, 1)

    // Elements: 10, 8 (x2), 6, 5, 2 (x2), 1
    // Modulo 2: 10, 6, 5, 1
    pq.iterator.toArray shouldBe Array(10, 6, 5, 1)
  }

  it should "work correctly with duplicate elements across many arrays" in {
    val pq = new LazyModulo2SortedArrayPriorityQueue[Int]()

    pq += Array(5, 3, 1)
    pq += Array(5, 4, 1)
    pq += Array(4, 3, 1)

    // Elements: 5(x2), 4(x2), 3(x2), 1(x3)
    // Modulo 2: 1
    pq.iterator.toArray shouldBe Array(1)
  }

  it should "be empty if all elements cancel out" in {
    val pq = new LazyModulo2SortedArrayPriorityQueue[Int]()

    pq += Array(10, 5, 1)
    pq += Array(10, 5, 1)

    pq.iterator.hasNext shouldBe false
  }

  it should "drain to array correctly" in {
    val pq = new LazyModulo2SortedArrayPriorityQueue[Int]()

    pq += Array(10, 8, 5)
    pq += Array(8, 2)

    pq.drainToArray() shouldBe Array(10, 5, 2)
    pq.iterator.hasNext shouldBe false
  }

  it should "allow adding elements during iteration" in {
    val genArrays = Gen.listOf(Gen.listOf(Gen.choose(1, 1000)).map(_.sorted.reverse.toArray))

    forAll(genArrays) { initialArrays =>
      val pq          = new LazyModulo2SortedArrayPriorityQueue[Int]()
      val allElements = mutable.ArrayBuffer.empty[Int]

      initialArrays.foreach { arr =>
        pq += arr
        allElements ++= arr
      }

      val result = mutable.ArrayBuffer.empty[Int]

      while (pq.nonEmpty) {
        val next = pq.peek.get

        // Randomly decide to add a new array that is <= next
        if (scala.util.Random.nextDouble() < 0.2) {
          val numElements = scala.util.Random.nextInt(10)
          val newArr = Array.fill(numElements)(scala.util.Random.nextInt(next + 1)).sorted.reverse
          if (newArr.nonEmpty) {
            pq += newArr
            allElements ++= newArr
          }
        } else {
          result += pq.dequeue()
        }
      }

      val expected = allElements
        .groupBy(identity)
        .mapValues(_.size % 2)
        .filter(_._2 != 0)
        .keys
        .toArray
        .sorted
        .reverse

      result.toArray shouldBe expected
    }
  }

}
