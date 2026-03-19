package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator

object EnclosingRadiusCalculator {

  /** Computes enclosing radius of a points cloud.
    *
    * Enclosing radius is minimal number for which there exists a point in the points cloud from
    * which distance to any other point is <= that number.
    *
    * In Vietoris–Rips filtration, homology becomes trivial after that radius.
    */
  def computeRadius(
    pointsCloud: Dataset[Array[Float]],
    distanceCalculator: DistanceCalculator
  ): Float = ???

}
