package io.github.jakipatryk.sparkpersistenthomology.matrixreduction

import io.github.jakipatryk.sparkpersistenthomology.utils.Empty
import io.github.jakipatryk.sparkpersistenthomology.{Chain, Key}
import io.github.jakipatryk.sparkpersistenthomology.utils.Empty
import io.github.jakipatryk.sparkpersistenthomology.{Chain, Key}

trait DataLoader {

  def nSeparateTriangles(n: Int): Iterator[(Key, Chain)] =
    (Range(0, 3 * n) map (i => (Key(i, None), Chain(Empty)))).iterator ++
      (Range(3 * n, 4 * n) map {
        i => (
          Key(i, Some((3 * (i - 3 * n) + 1) toLong)),
          Chain(((3 * (i - 3 * n) + 1) toLong) :: ((3 * (i - 3 * n)) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(4 * n, 5 * n) map {
        i => (
          Key(i, Some((3 * (i - 4 * n) + 2) toLong)),
          Chain(((3 * (i - 4 * n) + 2) toLong) :: ((3 * (i - 4 * n) + 1) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(5 * n, 6 * n) map {
        i => (
          Key(i, Some((3 * (i - 5 * n) + 2) toLong)),
          Chain(((3 * (i - 5 * n) + 2) toLong) :: ((3 * (i - 5 * n)) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(6 * n, 7 * n) map {
        i => (
          Key(i, Some(((i - 6 * n) + 3 * n + 2 * n) toLong)),
          Chain((((i - 6 * n) + 3 * n + 2 * n) toLong) :: (((i - 6 * n) + 3 * n + n) toLong) :: (((i - 6 * n) + 3 * n) toLong)  :: Nil)
        )
      }).iterator

  def nSeparateTrianglesExpectedReducedMatrix(n: Int): List[(Key, Chain)] = {
    val lineSegments = (Range(3 * n, 4 * n) map {
      i => (
        Key(i, Some((3 * (i - 3 * n) + 1) toLong)),
        Chain(((3 * (i - 3 * n) + 1) toLong) :: ((3 * (i - 3 * n)) toLong)  :: Nil)
      )
    }).iterator ++ (Range(4 * n, 5 * n) map {
      i => (
        Key(i, Some((3 * (i - 4 * n) + 2) toLong)),
        Chain(((3 * (i - 4 * n) + 2) toLong) :: ((3 * (i - 4 * n) + 1) toLong)  :: Nil)
      )
    }).iterator

    val triangleFillings = (Range(6 * n, 7 * n) map {
      i => (
        Key(i, Some(((i - 6 * n) + 3 * n + 2 * n) toLong)),
        Chain((((i - 6 * n) + 3 * n + 2 * n) toLong) :: (((i - 6 * n) + 3 * n + n) toLong) :: (((i - 6 * n) + 3 * n) toLong)  :: Nil)
      )
    }).iterator

    (lineSegments ++ triangleFillings).toList
  }

  def tetrahedron(): Iterator[(Key, Chain)] = {
    (Range(0, 4) map (i => (Key(i, None), Chain(Empty)))).iterator ++
      ((Key(4L, Some(1L)), Chain(1L :: 0L :: Nil))
        :: (Key(5L, Some(2L)), Chain(2L :: 1L :: Nil))
        :: (Key(6L, Some(3L)), Chain(3L :: 2L :: Nil))
        :: (Key(7L, Some(2L)), Chain(2L :: 0L :: Nil))
        :: (Key(8L, Some(3L)), Chain(3L :: 0L :: Nil))
        :: (Key(9L, Some(3L)), Chain(3L :: 1L :: Nil))
        :: (Key(10L, Some(9L)), Chain(9L :: 8L :: 4L :: Nil))
        :: (Key(11L, Some(9L)), Chain(9L :: 6L :: 5L :: Nil))
        :: (Key(12L, Some(8L)), Chain(8L :: 7L :: 6L :: Nil))
        :: (Key(13L, Some(7L)), Chain(7L :: 5L :: 4L :: Nil))
        :: (Key(14L, Some(13L)), Chain(13L :: 12L :: 11L :: 10L :: Nil))
        :: Nil).toIterator
  }

  def tetrahedronExpectedReducedMatrix(): List[(Key, Chain)] = {
    (Key(0L, None), Chain(Empty)) ::
      (Key(1L, None), Chain(Empty)) ::
      (Key(2L, None), Chain(Empty)) ::
      (Key(3L, None), Chain(Empty)) ::
      (Key(4L, Some(1L)), Chain(1L :: 0L :: Nil)) ::
      (Key(5L, Some(2L)), Chain(2L :: 1L :: Nil)) ::
      (Key(6L, Some(3L)), Chain(3L :: 2L :: Nil)) ::
      (Key(7L, None), Chain(Empty)) ::
      (Key(8L, None), Chain(Empty)) ::
      (Key(9L, None), Chain(Empty)) ::
      (Key(10L, Some(9L)), Chain(9L :: 8L :: 4L :: Nil)) ::
      (Key(11L, Some(8L)), Chain(8L :: 6L :: 5L :: 4L :: Nil)) ::
      (Key(12L, Some(7L)), Chain(7L :: 5L :: 4L :: Nil)) ::
      (Key(13L, None), Chain(Empty)) ::
      (Key(14L, Some(13L)), Chain(13L :: 12L :: 11L :: 10L :: Nil)) :: Nil
  }
}
