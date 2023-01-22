package io.github.jakipatryk.sparkpersistenthomology.utils

import scala.annotation.tailrec

/**
 * Implementation of sparse vector with coefficients in field Z2.
 * @param indicesOfOnes: list of indices of ones in the vector, sorted in decreasing order.
 */
class SparseVectorMod2(val indicesOfOnes: List[Long]) extends Serializable {

  def isEmpty: Boolean = indicesOfOnes.isEmpty

  def first: Option[Long] = indicesOfOnes match {
    case Nil => None
    case h :: _ => Some(h)
  }

  def +(other: SparseVectorMod2): SparseVectorMod2 = {
    @tailrec
    def aux(acc: List[Long], a: List[Long], b: List[Long]): List[Long] = {
      (a, b) match {
        case (Nil, Nil) => acc
        case (x :: restX, Nil) => aux(x :: acc, restX, Nil)
        case (Nil, y :: restY) => aux(y :: acc, Nil, restY)
        case (x :: restX, y :: restY) =>
          if(x == y) aux(acc, restX, restY)
          else if(x > y) aux(x :: acc, restX, b)
          else aux(y :: acc, a, restY)
      }
    }

    if(other.isEmpty) this
    else {
      val resultListOfIndices = aux(Nil, indicesOfOnes, other.indicesOfOnes).reverse
      if(resultListOfIndices.isEmpty) Empty
      else new SparseVectorMod2(resultListOfIndices)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case other: SparseVectorMod2 => indicesOfOnes == other.indicesOfOnes
    case _ => false
  }

  override def toString: String = indicesOfOnes.toString

}

object Empty extends SparseVectorMod2(Nil) {
  override def +(other: SparseVectorMod2): SparseVectorMod2 = other
}