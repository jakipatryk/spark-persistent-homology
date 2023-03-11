package io.github.jakipatryk

import org.apache.spark.rdd.RDD

package object sparkpersistenthomology {

  case class PointsCloud(rdd: RDD[Vector[Double]]) extends AnyVal

  trait Infinity

  object Infinity extends Infinity with Serializable

  case class PersistencePair(birth: Double, death: Either[Double, Infinity], dim: Int)

}
