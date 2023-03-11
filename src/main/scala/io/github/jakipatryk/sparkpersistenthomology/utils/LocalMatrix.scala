package io.github.jakipatryk.sparkpersistenthomology.utils

import scala.reflect.ClassTag

/** Somewhat efficient implementation of matrix of type `T`. */
class LocalMatrix[T : ClassTag] private(
                                         private val matrixFlattened: Array[T],
                                         val n: Int,
                                         val m: Int
                                       ) extends Serializable {

  /** Gets element of the matrix corresponding to `x`th row and `y`th column. */
  def apply(x: Int, y: Int): T = matrixFlattened(indices2DTo1D(x, y))

  /** Updates element of the matrix corresponding to `x`th row and `y`th column with `value`. */
  def update(x: Int, y: Int, value: T): Unit = {
    matrixFlattened(indices2DTo1D(x, y)) = value
  }

  private def indices2DTo1D(x: Int, y: Int): Int = x * m + y

}

object LocalMatrix {

  def zero[T : ClassTag](n: Int, m: Int)(implicit numeric: Numeric[T]): LocalMatrix[T] = {
    val matrixFlattened = Array.fill(n * m)(numeric.zero)
    new LocalMatrix[T](matrixFlattened, n, m)
  }

}
