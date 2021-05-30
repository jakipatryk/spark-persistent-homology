package com.jakipatryk.spark.persistenthomology

class Chain(val asVector: SparseVectorMod2) {
  def pivot: Option[Long] = asVector.first

  def +(other: Chain): Chain = new Chain(asVector + other.asVector)
}

case class Key(indexInMatrix: Long, pivot: Option[Long])
