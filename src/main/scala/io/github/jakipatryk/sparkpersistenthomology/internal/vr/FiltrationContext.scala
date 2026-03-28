package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

private[sparkpersistenthomology] case class FiltrationContext(
  cns: Broadcast[CombinatorialNumberSystem],
  pointsCloud: Broadcast[Array[Array[Float]]],
  distanceCalculator: DistanceCalculator,
  distanceThreshold: Float
)
