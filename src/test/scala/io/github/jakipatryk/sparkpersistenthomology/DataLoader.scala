package io.github.jakipatryk.sparkpersistenthomology

import io.github.jakipatryk.sparkpersistenthomology.filtrations.{IndexInMatrix, InitThreshold, SimplexBoundary}
import io.github.jakipatryk.sparkpersistenthomology.utils.Empty

trait DataLoader {

  def nSeparateTriangles(n: Int): Iterator[(IndexInMatrix, InitThreshold, SimplexBoundary)] =
    ((Range(0, 3 * n) map (i => (i toLong, 0.0, Chain(Empty)))).iterator ++
      (Range(3 * n, 4 * n) map {
        i => (
          i toLong,
          1.0,
          Chain(((3 * (i - 3 * n) + 1) toLong) :: ((3 * (i - 3 * n)) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(4 * n, 5 * n) map {
        i => (
          i toLong,
          2.0,
          Chain(((3 * (i - 4 * n) + 2) toLong) :: ((3 * (i - 4 * n) + 1) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(5 * n, 6 * n) map {
        i => (
          i toLong,
          3.0,
          Chain(((3 * (i - 5 * n) + 2) toLong) :: ((3 * (i - 5 * n)) toLong)  :: Nil)
        )
      }).iterator ++
      (Range(6 * n, 7 * n) map {
        i => (
          i toLong,
          4.0,
          Chain((((i - 6 * n) + 3 * n + 2 * n) toLong) :: (((i - 6 * n) + 3 * n + n) toLong) :: (((i - 6 * n) + 3 * n) toLong)  :: Nil)
        )
      }).iterator).map {
        case (index, threshold, boundary) =>
          (IndexInMatrix(index), InitThreshold(threshold), SimplexBoundary(boundary))
      }

  def nSeparateTrianglesExpectedPersistencePairs(n: Int): List[PersistencePair] = {
    val infiniteConnectedComponents = ((0 until n) map { i => PersistencePair(0.0, Right(Infinity), 0) }).toList
    val temporaryConnectedComponents =
      ((0 until n) map { i =>  PersistencePair(0.0, Left(1.0), 0) }).toList ++
        ((0 until n) map { i => PersistencePair(0.0, Left(2.0), 0) }).toList
    val temporaryHoles = ((0 until n) map { i => PersistencePair(3.0, Left(4.0), 1)}).toList

    infiniteConnectedComponents ++ temporaryConnectedComponents ++ temporaryHoles
  }

  def tetrahedron(): Iterator[(IndexInMatrix, InitThreshold, SimplexBoundary)] = {
    ((Range(0, 4) map (i => (i toLong, 0.0, Chain(Empty)))).iterator ++
      ((4L, 1.0, Chain(1L :: 0L :: Nil))
        :: (5L, 1.0, Chain(2L :: 1L :: Nil))
        :: (6L, 1.0, Chain(3L :: 2L :: Nil))
        :: (7L, 1.0, Chain(2L :: 0L :: Nil))
        :: (8L, 1.0, Chain(3L :: 0L :: Nil))
        :: (9L, 1.0, Chain(3L :: 1L :: Nil))
        :: (10L, 2.0, Chain(9L :: 8L :: 4L :: Nil))
        :: (11L, 3.0, Chain(9L :: 6L :: 5L :: Nil))
        :: (12L, 4.0, Chain(8L :: 7L :: 6L :: Nil))
        :: (13L, 5.0, Chain(7L :: 5L :: 4L :: Nil))
        :: (14L, 10.0, Chain(13L :: 12L :: 11L :: 10L :: Nil))
        :: Nil).toIterator).map {
      case (index, threshold, boundary) =>
        (IndexInMatrix(index), InitThreshold(threshold), SimplexBoundary(boundary))
    }
  }

  def tetrahedronExpectedPersistencePairs(): List[PersistencePair] = {
    PersistencePair(0.0, Right(Infinity), 0) ::
      PersistencePair(0.0, Left(1.0), 0) ::
      PersistencePair(0.0, Left(1.0), 0) ::
      PersistencePair(0.0, Left(1.0), 0) ::
      PersistencePair(1.0, Left(4.0), 1) ::
      PersistencePair(1.0, Left(3.0), 1) ::
      PersistencePair(1.0, Left(2.0), 1) ::
      PersistencePair(5.0, Left(10.0), 2) ::
      Nil
  }
}