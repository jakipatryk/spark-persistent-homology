package io.github.jakipatryk.sparkpersistenthomology

import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.internal.flag.FlagFiltrationPersistentCohomology

object PersistentHomology {

  /** Computes persistent homology for dimensions up to `maxDim`.
    *
    * @param pointsCloud
    *   A Spark Dataset where each element is an array of floats representing a point in space.
    * @param maxDim
    *   Maximum dimension of homology to compute (inclusive).
    * @param config
    *   Configuration for the filtration (e.g., Vietoris-Rips or Nearest-Neighbors).
    * @return
    *   An array of Spark Datasets where each index 'i' contains the persistence pairs for homology
    *   in dimension 'i'.
    */
  def computePersistentHomology(
    pointsCloud: Dataset[Array[Float]],
    maxDim: Int,
    config: FiltrationConfig = FiltrationConfig.VietorisRips()
  ): Array[Dataset[PersistencePair]] = {
    implicit val spark = pointsCloud.sparkSession
    FlagFiltrationPersistentCohomology.computePersistencePairs(
      pointsCloud,
      maxDim,
      config
    )
  }

}
