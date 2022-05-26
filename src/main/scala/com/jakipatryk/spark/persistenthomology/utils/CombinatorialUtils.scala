package com.jakipatryk.spark.persistenthomology.utils

import org.apache.spark.rdd.RDD

object CombinatorialUtils extends Serializable {

  /**
   * Computes function f for all combinations of elements of an RDD,
   * up to combinations containing n elements
   * @param rdd RDD of any type used to generate combinations of its elements
   * @param n Integer indicating what is the max number of elements in any combination
   * @param f Function to be computed on combinations
   * @return RDD containing results of application of f
   *         on each combination and indices used in this combination
   */
  def computeForAllCombinationsUpToN[T, V](
                                            rdd: RDD[T],
                                            n: Int,
                                            f: List[(T, Long)] => V
                                          ): RDD[(V, List[Long])] = {
    val withIndex = rdd.zipWithIndex().cache()
    var finalRdd = withIndex.map {
      case (value, index) =>
        val l = List((value, index))
        (f(l), l)
    }
    var prevRdd = finalRdd

    for (_ <- 0 until (n - 1)) {
      prevRdd = prevRdd
        .cartesian(withIndex)
        .filter { case ((_, l), (_, i)) => l.head._2 < i }
        .map {
          case ((_, list), (value, index)) =>
            val newList = (value, index) :: list
            (f(newList), newList)
        }
      finalRdd = finalRdd union prevRdd
    }

    finalRdd.map {
      case (value, list) => (value, list.map { case (_, index) => index })
    }
  }

}
