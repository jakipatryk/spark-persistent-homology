package io.github.jakipatryk.sparkpersistenthomology

import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology.PersistenceIndicesPair
import io.github.jakipatryk.sparkpersistenthomology.filtrations.Filtration
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class PersistentHomologySpec extends AnyFlatSpec with DataLoader with BeforeAndAfterAll {

  implicit var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("PersistentHomologySpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  private def multisetEqual(l1: List[_], l2: List[_]): Boolean =
    (l1.length == l2.length) && l1.forall(x => l2 contains x)

  "getPersistencePairs(filtration)" should "return all finite and infinite persistence pairs for 10 triangles" in {
    val data = sparkContext.parallelize(nSeparateTriangles(10).toList)
    val filtration = Filtration(data)

    val result =
      PersistentHomology
        .getPersistencePairs(filtration, 8)
        .collect()
        .toList

    val expected = nSeparateTrianglesExpectedPersistencePairs(10)

    assert(multisetEqual(result, expected))
  }

  "getPersistencePairs(filtration)" should
    "return all finite and infinite persistence pairs for tetrahedron" in {
    val data = sparkContext.parallelize(tetrahedron().toList)
    val filtration = Filtration(data)

    val result =
      PersistentHomology
        .getPersistencePairs(filtration, 4)
        .collect()
        .toList

    val expected = tetrahedronExpectedPersistencePairs()
    assert(multisetEqual(result, expected))
  }

  "getPersistencePairs(filtration)" should
    "work with version of function with automatically determined number of partitions" in {
    val data = sparkContext.parallelize(tetrahedron().toList)
    val filtration = Filtration(data)

    val result =
      PersistentHomology
        .getPersistencePairs(filtration)
        .collect()
        .toList

    val expected = tetrahedronExpectedPersistencePairs()
    assert(multisetEqual(result, expected))
  }

  "getPersistencePairs(pointsCloud)" should
    "return all finite and infinite persistence pairs for three points and maxDim=1" in {
    val data = sparkContext.parallelize(
      Vector(0.0, 0.0, 0.0) :: Vector(1.0, 1.0, 1.0) :: Vector(2.0, 2.0, 2.0) :: Nil
    )
    val pointsCloud = PointsCloud(data)

    val result =
      PersistentHomology
        .getPersistencePairs(pointsCloud, Some(4), Some(1))
        .collect()
        .toList

    val expected = PersistencePair(0.0, Right(Infinity), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      PersistencePair(math.sqrt(12.0), Right(Infinity), 1) ::
      Nil
    assert(multisetEqual(result, expected))
  }

  "getPersistencePairs(pointsCloud)" should
    "return all finite and infinite persistence pairs for three points and maxDim=2" in {
    val data = sparkContext.parallelize(
      Vector(0.0, 0.0, 0.0) :: Vector(1.0, 1.0, 1.0) :: Vector(2.0, 2.0, 2.0) :: Nil
    )
    val pointsCloud = PointsCloud(data)

    val result =
      PersistentHomology
        .getPersistencePairs(pointsCloud, Some(4), Some(2))
        .collect()
        .toList

    val expected = PersistencePair(0.0, Right(Infinity), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      Nil
    assert(multisetEqual(result, expected))
  }

  "getPersistencePairs(pointsCloud)" should
    "work with version of function with automatically determined number of partitions" in {
    val data = sparkContext.parallelize(
      Vector(0.0, 0.0, 0.0) :: Vector(1.0, 1.0, 1.0) :: Vector(2.0, 2.0, 2.0) :: Nil
    )
    val pointsCloud = PointsCloud(data)

    val result =
      PersistentHomology
        .getPersistencePairs(pointsCloud, maxDim = Some(2))
        .collect()
        .toList

    val expected = PersistencePair(0.0, Right(Infinity), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      PersistencePair(0.0, Left(math.sqrt(3.0)), 0) ::
      Nil
    assert(multisetEqual(result, expected))
  }

  "getInfinitePairs" should "return correct pairs if indices at the beginning of range are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(3L, Left(5L)) :: PersistenceIndicesPair(4L, Left(6L)) :: Nil
    )
    val filtrationLength = 7L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(0L, Right(Infinity)),
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(2L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if indices at the end of range are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(0L, Left(2L)) :: PersistenceIndicesPair(1L, Left(3L)) :: Nil
    )
    val filtrationLength = 7L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(4L, Right(Infinity)),
      PersistenceIndicesPair(5L, Right(Infinity)),
      PersistenceIndicesPair(6L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if indices in the middle of finite indices are infinite" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(0L, Left(2L)) :: PersistenceIndicesPair(4L, Left(7L)) :: Nil
    )
    val filtrationLength = 8L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(3L, Right(Infinity)),
      PersistenceIndicesPair(5L, Right(Infinity)),
      PersistenceIndicesPair(6L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if there are no finite pairs in partition" in {
    val finitePairs = sparkContext.parallelize(List[PersistenceIndicesPair]())
    val filtrationLength = 5L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(0L, Right(Infinity)),
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(2L, Right(Infinity)),
      PersistenceIndicesPair(3L, Right(Infinity)),
      PersistenceIndicesPair(4L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs if infinite indices are at the beginning, middle and end" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(2L, Left(5L)) :: PersistenceIndicesPair(6L, Left(7L)) :: Nil
    )
    val filtrationLength = 9L
    val numOfPartitions = 1

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(0L, Right(Infinity)),
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(3L, Right(Infinity)),
      PersistenceIndicesPair(4L, Right(Infinity)),
      PersistenceIndicesPair(8L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs when there are multiple partitions and each is empty" in {
    val finitePairs = sparkContext.parallelize(List[PersistenceIndicesPair]())
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(0L, Right(Infinity)),
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(2L, Right(Infinity)),
      PersistenceIndicesPair(3L, Right(Infinity)),
      PersistenceIndicesPair(4L, Right(Infinity)),
      PersistenceIndicesPair(5L, Right(Infinity)),
      PersistenceIndicesPair(6L, Right(Infinity)),
      PersistenceIndicesPair(7L, Right(Infinity)),
      PersistenceIndicesPair(8L, Right(Infinity)),
      PersistenceIndicesPair(9L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return correct pairs when there are multiple partitions and some are empty" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(3L, Left(5L)) :: Nil
    )
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set(
      PersistenceIndicesPair(0L, Right(Infinity)),
      PersistenceIndicesPair(1L, Right(Infinity)),
      PersistenceIndicesPair(2L, Right(Infinity)),
      PersistenceIndicesPair(4L, Right(Infinity)),
      PersistenceIndicesPair(6L, Right(Infinity)),
      PersistenceIndicesPair(7L, Right(Infinity)),
      PersistenceIndicesPair(8L, Right(Infinity)),
      PersistenceIndicesPair(9L, Right(Infinity))
    )
    assert(result == expected)
  }

  "getInfinitePairs" should "return no pairs when there are multiple partitions and each is full" in {
    val finitePairs = sparkContext.parallelize(
      PersistenceIndicesPair(0L, Left(1L)) ::
      PersistenceIndicesPair(2L, Left(3L)) ::
      PersistenceIndicesPair(4L, Left(5L)) ::
      PersistenceIndicesPair(8L, Left(9L)) ::
      PersistenceIndicesPair(6L, Left(7L)) :: Nil
    )
    val filtrationLength = 10L
    val numOfPartitions = 3

    val result = PersistentHomology
      .getInfinitePairs(finitePairs, filtrationLength, numOfPartitions)
      .collect()
      .toSet

    val expected = Set[PersistenceIndicesPair]()
    assert(result == expected)
  }

}
