package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Dataset
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

object CoboundaryMatrixConstructor {

  /** Constructs `Dataset` of [[CoboundaryMatrixColumn]] for columns that need to be reduced.
    *
    * It skips columns that are known to become zero after reduction (clearing optimization and
    * apparent pairs).
    */
  def construct(
    cns: CombinatorialNumberSystem,
    distanceCalculator: DistanceCalculator,
    distanceThreshold: Float,
    previousDimPivots: Option[Dataset[Long]] = None
  ): Dataset[CoboundaryMatrixColumn] = ???

}
