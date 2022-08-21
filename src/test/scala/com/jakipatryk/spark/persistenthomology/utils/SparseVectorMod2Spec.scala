package com.jakipatryk.spark.persistenthomology.utils

import org.scalatest.flatspec.AnyFlatSpec

class SparseVectorMod2Spec extends AnyFlatSpec {

  "Empty" should "be the unit for + operation" in {
    val vec = new SparseVectorMod2(10L :: 3L :: 2L :: Nil)

    assert(vec + Empty == vec)
    assert(Empty + vec == vec)
    assert(Empty + Empty == Empty)
  }

  "+ of two equal vectors" should "result in Empty vector" in {
    val vec = new SparseVectorMod2(10L :: 3L :: 2L :: Nil)
    assert(vec + vec == Empty)
  }

  "+" should "add two vectors modulo 2" in {
    val vec1 = new SparseVectorMod2(10L :: 3L :: 2L :: Nil)
    val vec2 = new SparseVectorMod2(10L :: 4L :: Nil)
    val vec3 = new SparseVectorMod2(3L :: 1L :: Nil)
    val vec4 = new SparseVectorMod2(11L :: 5L :: 4L :: 1L :: Nil)

    assert(vec1 + vec2 == new SparseVectorMod2(4L :: 3L :: 2L :: Nil))
    assert(vec1 + vec3 == new SparseVectorMod2(10L :: 2L :: 1L :: Nil))
    assert(vec1 + vec4 == new SparseVectorMod2(11L :: 10L :: 5L :: 4L :: 3L :: 2L :: 1L :: Nil))
    assert(vec2 + vec3 == new SparseVectorMod2(10L :: 4L :: 3L :: 1L :: Nil))
    assert(vec2 + vec4 == new SparseVectorMod2(11L :: 10L :: 5L :: 1L :: Nil))
    assert(vec3 + vec4 == new SparseVectorMod2(11L :: 5L :: 4L :: 3L :: Nil))
  }

}
