package com.jakipatryk.spark.persistenthomology

import com.jakipatryk.spark.persistenthomology.utils.SparseVectorMod2

class Chain(val asVector: SparseVectorMod2) extends Serializable {
  def pivot: Option[Long] = asVector.first

  def +(other: Chain): Chain = new Chain(asVector + other.asVector)

  override def toString: String = s"Chain(IndicesOfOnes(${asVector.toString}))"

  def isEmpty: Boolean = asVector.isEmpty

  override def equals(obj: Any): Boolean = obj match {
    case c: Chain => asVector == c.asVector
    case _ => false
  }
}

object Chain {

  def apply(asVector: SparseVectorMod2): Chain = new Chain(asVector)

  def apply(asListOfOnes: List[Long]) = new Chain(new SparseVectorMod2(asListOfOnes))

}

case class Key(indexInMatrix: Long, pivot: Option[Long])
