package com.jakipatryk.spark.persistenthomology

import org.apache.spark.rdd.RDD

package object filtrations {

  type InitThreshold = Double

  type SimplexBoundary = Chain

  type IndexInMatrix = Long

  /**
   * Type alias for filtration.
   * IMPORTANT: filtration is expected to be sorted by (InitThreshold, DimOfSimplex)
   */
  type Filtration = RDD[(IndexInMatrix, InitThreshold, SimplexBoundary)]

  /**
   * Type alias for points cloud.
   */
  type PointsCloud = RDD[Vector[Double]]

}
