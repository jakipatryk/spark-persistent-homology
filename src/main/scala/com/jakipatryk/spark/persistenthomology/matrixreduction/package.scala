package com.jakipatryk.spark.persistenthomology

import org.apache.spark.rdd.RDD

package object matrixreduction {

  type BoundaryMatrix = RDD[(Key, Chain)]

}
