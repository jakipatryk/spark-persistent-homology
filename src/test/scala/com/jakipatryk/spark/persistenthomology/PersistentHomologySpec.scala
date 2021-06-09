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

  "getInfinitePairs" should "return correct pairs if indices at the beginning of range are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(3L, Left(5L)) :: PersistencePair(4L, Left(6L)) :: Nil
    )
    val filtrationLength = 7L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(0L, Right(Infinity)),
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(2L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if indices at the end of range are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(0L, Left(2L)) :: PersistencePair(1L, Left(3L)) :: Nil
    )
    val filtrationLength = 7L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(4L, Right(Infinity)),
      PersistencePair(5L, Right(Infinity)),
      PersistencePair(6L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if indices in the middle of finite indices are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(0L, Left(2L)) :: PersistencePair(4L, Left(7L)) :: Nil
    )
    val filtrationLength = 8L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(3L, Right(Infinity)),
      PersistencePair(5L, Right(Infinity)),
      PersistencePair(6L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if there are no finite pairs in partition" in {
    val finitePairs = sparkContext.parallelize(List[PersistencePair]())
    val filtrationLength = 5L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(0L, Right(Infinity)),
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(2L, Right(Infinity)),
      PersistencePair(3L, Right(Infinity)),
      PersistencePair(4L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if infinite indices are at the beginning, middle and end" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(2L, Left(5L)) :: PersistencePair(6L, Left(7L)) :: Nil
    )
    val filtrationLength = 9L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(0L, Right(Infinity)),
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(3L, Right(Infinity)),
      PersistencePair(4L, Right(Infinity)),
      PersistencePair(8L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs when there are multiple partitions and each is empty" in {
    val finitePairs = sparkContext.parallelize(List[PersistencePair]())
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(0L, Right(Infinity)),
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(2L, Right(Infinity)),
      PersistencePair(3L, Right(Infinity)),
      PersistencePair(4L, Right(Infinity)),
      PersistencePair(5L, Right(Infinity)),
      PersistencePair(6L, Right(Infinity)),
      PersistencePair(7L, Right(Infinity)),
      PersistencePair(8L, Right(Infinity)),
      PersistencePair(9L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs when there are multiple partitions and some are empty" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(3L, Left(5L)) :: Nil
    )
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistencePair(0L, Right(Infinity)),
      PersistencePair(1L, Right(Infinity)),
      PersistencePair(2L, Right(Infinity)),
      PersistencePair(4L, Right(Infinity)),
      PersistencePair(6L, Right(Infinity)),
      PersistencePair(7L, Right(Infinity)),
      PersistencePair(8L, Right(Infinity)),
      PersistencePair(9L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return no pairs when there are multiple partitions and each is full" in {
    val finitePairs = sparkContext.parallelize(
      PersistencePair(0L, Left(1L)) ::
      PersistencePair(2L, Left(3L)) ::
      PersistencePair(4L, Left(5L)) ::
      PersistencePair(8L, Left(9L)) ::
      PersistencePair(6L, Left(7L)) :: Nil
    )
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set[PersistencePair]()
    assert(result == expected)
  }


}
