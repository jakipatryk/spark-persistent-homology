package com.jakipatryk.spark.persistenthomology

import org.apache.spark.rdd.RDD

package object filtrations {

  /** Represents index in boundary matrix of simplex in filtration. */
  case class IndexInMatrix(index: Long) extends AnyVal

  /** Represents threshold when simplex appears in filtration. */
  case class InitThreshold(threshold: Double) extends AnyVal

  /** Represents boundary of simplex. */
  case class SimplexBoundary(boundary: Chain) extends AnyVal

  /**
   * Represents filtration (a sequence of simplices such that any prefix of it is a simplicial complex).
   * Simplex boundary is with respect to indices of these simplices in boundary matrix.
   * IMPORTANT: Filtration is expected to be sorted by (InitThreshold, DimOfSimplex),
   *            thus Filtration is ready to be converted to boundary matrix.
   */
  case class Filtration(rdd: RDD[(IndexInMatrix, InitThreshold, SimplexBoundary)]) extends AnyVal

  case class PointsCloud(rdd: RDD[Vector[Double]]) extends AnyVal

}
