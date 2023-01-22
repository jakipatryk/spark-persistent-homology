package io.github.jakipatryk.sparkpersistenthomology

import org.apache.spark.rdd.RDD

package object matrixreduction {

  case class BoundaryMatrix(rdd: RDD[(Key, Chain)]) extends AnyVal

}
