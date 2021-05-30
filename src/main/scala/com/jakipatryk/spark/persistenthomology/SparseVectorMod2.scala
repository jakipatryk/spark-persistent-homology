package com.jakipatryk.spark.persistenthomology

/**
 * Implementation of spare vector with coefficients in field Z2.
 * @param indicesOfOnes: list of indices of ones in the vector, sorted in decreasing order.
 */
class SparseVectorMod2(val indicesOfOnes: List[Long]) {

  def isEmpty: Boolean = indicesOfOnes.isEmpty

  def first: Option[Long] = indicesOfOnes match {
    case Nil => None
    case h :: _ => Some(h)
  }

  def +(other: SparseVectorMod2): SparseVectorMod2 = {
    def aux(a: List[Long], b: List[Long]): List[Long] = {
      (a, b) match {
        case (Nil, Nil) => Nil
        case (x :: rest, Nil) => x :: aux(rest, b)
        case (Nil, y :: rest) => y :: aux(a, rest)
        case (x :: rest1, y :: rest2) =>
          if(x == y) aux(rest1, rest2)
          else if(x > y) x :: aux(rest1, b)
          else y :: aux(a, rest2)
      }
    }

    new SparseVectorMod2(aux(indicesOfOnes, other.indicesOfOnes))
  }

  override def equals(other: Any): Boolean = other match {
    case other: SparseVectorMod2 => indicesOfOnes == other.indicesOfOnes
    case _ => false
  }

  override def toString: String = indicesOfOnes.toString

}

object Empty extends SparseVectorMod2(Nil)
