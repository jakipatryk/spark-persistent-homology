package io.github.jakipatryk.sparkpersistenthomology.filtrations

import io.github.jakipatryk.sparkpersistenthomology.PointsCloud
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import org.apache.spark.SparkContext

trait FiltrationCreator extends Serializable {

  /**
   * Creates a filtration based on points cloud.
   * @param pointsCloud RDD of Vectors of Doubles, in other words a set of points
   * @param maxDim (default None) max dimension of simplices in a filtration;
   *               dimension of a simplex = (number of points that define it) - 1
   * @param distanceCalculator (default EuclideanDistanceCalculator)
   * @return Filtration
   */
  def createFiltration(
                        pointsCloud: PointsCloud,
                        maxDim: Option[Int] = None,
                        distanceCalculator: DistanceCalculator = DistanceCalculator.EuclideanDistanceCalculator
                      )(implicit sparkContext: SparkContext): Filtration

}
