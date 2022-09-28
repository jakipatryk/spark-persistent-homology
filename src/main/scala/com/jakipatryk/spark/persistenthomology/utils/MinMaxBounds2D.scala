package com.jakipatryk.spark.persistenthomology.utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Represents minimum and maximum values in a single dimension.
 * @tparam T type that has implicit Numeric implementation (like Double, Int, etc.)
 */
case class SingleDimMinMaxBound[T](min: T, max: T)(implicit n: Numeric[T]) {

  /**
   * Merges two SingleDimMinMaxBound such that resulting SingleDimMinMaxBound has
   * min equal to min of the two input mins
   * and max equal to max of the two input maxes.
   */
  def merge(other: SingleDimMinMaxBound[T]): SingleDimMinMaxBound[T] =
    SingleDimMinMaxBound(n.min(min, other.min), n.max(max, other.max))

}

/**
 * Represents minimum and maximum values in each of two dimensions.
 * @tparam T type that has implicit Numeric implementation (like Double, Int, etc.)
 */
case class MinMaxBounds2D[T](xBound: SingleDimMinMaxBound[T], yBound: SingleDimMinMaxBound[T])(implicit n: Numeric[T]) {

  /**
   * Merges two MinMaxBounds2D such that resulting MinMaxBounds2D has
   * xBound equal to merged xBound's of the two inputs, similarly yBound.
   * @see [[SingleDimMinMaxBound.merge]]
   */
  def merge(other: MinMaxBounds2D[T]): MinMaxBounds2D[T] =
    MinMaxBounds2D[T](xBound merge other.xBound, yBound merge other.yBound)

}

object MinMaxBounds2D {

  /**
   * Creates MinMaxBounds2D from a single point (x, y).
   */
  def fromSinglePoint[T](x: T, y: T)(implicit n: Numeric[T]): MinMaxBounds2D[T] =
    MinMaxBounds2D(SingleDimMinMaxBound(x, x), SingleDimMinMaxBound(y, y))

  /**
   * For each key in RDD calculates MinMaxBounds2D of its values.
   * @param rdd key-value RDD where value is a pair
   * @tparam K type for key
   * @tparam T type that has implicit Numeric implementation (like Double, Int, etc.)
   * @return Map where key is of the same type as in RDD and value is of type MinMaxBounds2D
   *         representing mapping key -> bounds.
   */
  def boundsPerKeyFromRDD[K, T](rdd: RDD[(K, (T, T))])
                               (implicit n: Numeric[T], kt: ClassTag[K]): Map[K, MinMaxBounds2D[T]] =
    rdd
      .aggregateByKey[Option[MinMaxBounds2D[T]]](None)(
        {
          case (None, current) => Some(fromSinglePoint(current._1, current._2))
          case (Some(acc), current) => Some(acc merge fromSinglePoint(current._1, current._2))
        },
        {
          case (None, bounds) => bounds
          case (bounds, None) => bounds
          case (Some(aBounds), Some(bBounds)) => Some(aBounds merge bBounds)
        }
      )
      .collect()
      .map { case (k, Some(bounds)) => (k, bounds) }
      .toMap

}
