package com.jakipatryk.spark.persistenthomology

import scala.annotation.tailrec

/**
 * Implementation of spare vector with coefficients in field Z2.
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

    new SparseVectorMod2(aux(Nil, indicesOfOnes, other.indicesOfOnes).reverse)
  }

  override def equals(other: Any): Boolean = other match {
    case other: SparseVectorMod2 => indicesOfOnes == other.indicesOfOnes
    case _ => false
  }

  override def toString: String = indicesOfOnes.toString

}

object Empty extends SparseVectorMod2(Nil)
