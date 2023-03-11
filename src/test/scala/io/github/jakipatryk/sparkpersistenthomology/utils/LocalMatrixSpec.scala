package io.github.jakipatryk.sparkpersistenthomology.utils

import org.scalatest.flatspec.AnyFlatSpec

class LocalMatrixSpec extends AnyFlatSpec {

  behavior of "LocalMatrix.zero"

  it should "create matrix with all zeros for Int" in {
    val localMatrix = LocalMatrix.zero[Int](5, 5)

    val zero: Int = 0

    for {
      i <- 0 until 5
      j <- 0 until 5
    } assert(localMatrix(i, j) === zero)
  }

  it should "create matrix with all zeros for Double" in {
    val localMatrix = LocalMatrix.zero[Double](5, 5)

    val zero: Double = 0.0

    for {
      i <- 0 until 5
      j <- 0 until 5
    } assert(localMatrix(i, j) === zero)
  }

  behavior of "LocalMatrix"

  it should "have value updated after `update` is called" in {
    val localMatrix = LocalMatrix.zero[Int](5, 5)

    localMatrix(0, 0) = 10
    localMatrix(2, 3) = 15
    localMatrix(4, 4) = -20

    assert(localMatrix(0, 0) === 10)
    assert(localMatrix(2, 3) === 15)
    assert(localMatrix(4, 4) === -20)
  }

}
