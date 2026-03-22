package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast
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
    broadcastedPoints: Broadcast[Array[Array[Float]]],
    distanceCalculator: DistanceCalculator
  ): Float = {
    if (pointsCloud.isEmpty) {
      return 0.0f
    }

    import org.apache.spark.sql.functions.min
    import pointsCloud.sparkSession.implicits._

    pointsCloud.map { p =>
      broadcastedPoints.value.map(distanceCalculator.calculateDistance(p, _)).max
    }
      .agg(min("value"))
      .as[Float]
      .first()
  }

}
