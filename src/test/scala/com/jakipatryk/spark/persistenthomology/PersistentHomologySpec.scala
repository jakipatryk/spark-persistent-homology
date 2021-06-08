package com.jakipatryk.spark.persistenthomology

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec

class PersistentHomologySpec extends AnyFlatSpec with DataLoader {
  val sparkContext = new SparkContext(
    new SparkConf().setAppName("PersistentHomologySpec").setMaster("local[*]")
  )

  "getPersistencePairs" should "return all finite and infinite persistence pairs for 10 triangles" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)

    val result =
      PersistentHomology
        .getPersistencePairs(data, 8)
        .collect()
        .toList
        .sortBy { case PersistencePair(i, _) => i }

    val expected = nSeparateTrianglesExpectedPersistencePairs(10).sortBy { case PersistencePair(i, _) => i }
    assert(result == expected)
  }

  "getPersistencePairs" should "return all finite and infinite persistence pairs for tetrahedron" in {
    val data = sparkContext.parallelize(tetrahedron().toList)

    val result =
      PersistentHomology
        .getPersistencePairs(data, 4)
        .collect()
        .toList
        .sortBy { case PersistencePair(i, _) => i }

    val expected = tetrahedronExpectedPersistencePairs().sortBy { case PersistencePair(i, _) => i }
    assert(result == expected)
  }


}
