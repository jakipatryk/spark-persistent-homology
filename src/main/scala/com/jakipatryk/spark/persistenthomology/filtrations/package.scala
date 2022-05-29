package com.jakipatryk.spark.persistenthomology

import org.apache.spark.rdd.RDD

package object filtrations {

  type InitThreshold = Double

  type SimplexBoundary = Chain

  type IndexInMatrix = Long

  /**
   * Type alias for filtration.
   * Simplex boundary is with respect to indices of these simplices in boundary matrix.
   * IMPORTANT: Filtration is expected to be sorted by (InitThreshold, DimOfSimplex),
   *            thus Filtration is ready to be converted to boundary matrix.
   */
  type Filtration = RDD[(IndexInMatrix, InitThreshold, SimplexBoundary)]

  /**
   * Type alias for points cloud.
   */
  type PointsCloud = RDD[Vector[Double]]

}
