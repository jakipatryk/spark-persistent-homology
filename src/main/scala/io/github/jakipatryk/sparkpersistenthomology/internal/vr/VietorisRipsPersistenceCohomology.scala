package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import org.apache.spark.sql.SparkSession

private[sparkpersistenthomology] object VietorisRipsPersistentCohomology {

  def computePersistencePairs(
    pointsCloud: Dataset[Array[Float]],
    maxDim: Int,
    distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator,
    distanceThreshold: Option[Float] = None
  )(implicit spark: SparkSession): Array[Dataset[PersistencePair]] = {
    ???
  }

}
