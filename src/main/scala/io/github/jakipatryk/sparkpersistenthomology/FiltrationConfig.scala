package io.github.jakipatryk.sparkpersistenthomology

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator

sealed trait FiltrationConfig

object FiltrationConfig {

  /** Configuration for Vietoris-Rips filtration.
    *
    * @param distanceCalculator
    *   The metric used to calculate distances between points. Defaults to Euclidean distance.
    * @param distanceThreshold
    *   Optional maximum distance for the filtration.
    */
  case class VietorisRips(
    distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator,
    distanceThreshold: Option[Float] = None
  ) extends FiltrationConfig

  /** Configuration for Nearest Neighbors filtration.
    *
    * @param k
    *   The number of nearest neighbors to compute.
    * @param distanceCalculator
    *   The metric used to calculate distances between points. Defaults to Euclidean distance.
    */
  case class NearestNeighbors(
    k: Int,
    distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
  ) extends FiltrationConfig

}
