package io.github.jakipatryk.sparkpersistenthomology

import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator

object PersistentHomology {

  /** Computes persistent homology for dimensions up to `maxDim`.
    *
    * @param pointsCloud
    *   A Spark Dataset where each element is an array of floats representing a point in space.
    * @param maxDim
    *   Maximum dimension of homology to compute (inclusive).
    * @param distanceCalculator
    *   The metric used to calculate distances between points. Defaults to Euclidean distance.
    * @param distanceThreshold
    *   Optional maximum distance for the filtration. Simplices with a radius larger than this will
    *   not be included.
    * @return
    *   An array of Spark Datasets where each index 'i' contains the persistence pairs for homology
    *   in dimension 'i'.
    */
  def computePersistentHomology(
    pointsCloud: Dataset[Array[Float]],
    maxDim: Int,
    distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator,
    distanceThreshold: Option[Float] = None
  ): Array[Dataset[PersistencePair]] = {
    ???
  }

}
