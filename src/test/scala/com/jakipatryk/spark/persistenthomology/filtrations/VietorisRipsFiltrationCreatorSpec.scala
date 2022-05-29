package com.jakipatryk.spark.persistenthomology.filtrations

import com.jakipatryk.spark.persistenthomology.{Chain, PersistentHomology}
import com.jakipatryk.spark.persistenthomology.utils.Empty
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VietorisRipsFiltrationCreatorSpec extends AnyFlatSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("VietorisRipsFiltrationCreatorSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  lazy val threePoints: RDD[Vector[Double]] = sparkContext
    .parallelize(Vector(0.0, 0.0, 0.0) :: Vector(1.0, 0.0, 0.0) :: Vector(0.0, 2.0, 0.0) :: Nil)

  lazy val fourPoints: RDD[Vector[Double]] = sparkContext
    .parallelize(
      Vector(0.0, 0.0, 0.0)
        :: Vector(1.0, 0.0, 0.0)
        :: Vector(5.0, 5.0, 5.0)
        :: Vector(0.0, 2.0, 0.0)
        :: Nil
    )

  "createFiltration" should "create proper filtration for three points and maxDim=2" in {
    val pointCloud = threePoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(2))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 7)
    for (i <- 0 to 2) assert(filtration(i) == (i, 0.0, Chain(Empty)))
    assert(filtration(3) == (3, 1.0, Chain(List(1L, 0L))))
    assert(filtration(4) == (4, 2.0, Chain(List(2L, 0L))))
    assert(filtration(5) == (5, math.sqrt(5.0), Chain(List(2L, 1L))))
    assert(filtration(6) == (6, math.sqrt(5.0), Chain(List(5L, 4L, 3L))))
  }

  "createFiltration" should
    "create proper the same filtration for three points with maxDim=None as if maxDim=2" in {
    val pointCloud = threePoints

    val filtrationMaxDim = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(2))
      .collect()
      .sortBy(_._1)
    val filtrationNoMaxDim = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud)
      .collect()
      .sortBy(_._1)

    assert(filtrationMaxDim.length == filtrationNoMaxDim.length)
    for (i <- 0 to 6) assert(filtrationMaxDim(i) == filtrationNoMaxDim(i))
  }

  "createFiltration" should "create proper filtration for three points and maxDim=1" in {
    val pointCloud = threePoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(1))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 6)
    for (i <- 0 to 2) assert(filtration(i) == (i, 0.0, Chain(Empty)))
    assert(filtration(3) == (3, 1.0, Chain(List(1L, 0L))))
    assert(filtration(4) == (4, 2.0, Chain(List(2L, 0L))))
    assert(filtration(5) == (5, math.sqrt(5.0), Chain(List(2L, 1L))))
  }

  "createFiltration" should "create proper filtration for three points and maxDim=0" in {
    val pointCloud = threePoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(0))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 3)
    for (i <- 0 to 2) assert(filtration(i) == (i, 0.0, Chain(Empty)))
  }

  "createFiltration" should "create proper filtration for four points and maxDim=3" in {
    val pointCloud = fourPoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(3))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 15)
    for (i <- 0 to 3) assert(filtration(i) == (i, 0.0, Chain(Empty)))
    assert(filtration(4) == (4, 1.0, Chain(List(1L, 0L))))
    assert(filtration(5) == (5, 2.0, Chain(List(3L, 0L))))
    assert(filtration(6) == (6, math.sqrt(5.0), Chain(List(3L, 1L))))
    assert(filtration(7) == (7, math.sqrt(5.0), Chain(List(6L, 5L, 4L))))
    assert(filtration(8) == (8, math.sqrt(25.0 + 9.0 + 25.0), Chain(List(3L, 2L))))
    assert(filtration(9) == (9, math.sqrt(16.0 + 25.0 + 25.0), Chain(List(2L, 1L))))
    assert(filtration(10) == (10, math.sqrt(16.0 + 25.0 + 25.0), Chain(List(9L, 8L, 6L))))
    assert(filtration(11) == (11, math.sqrt(25.0 + 25.0 + 25.0), Chain(List(2L, 0L))))
    assert(
      filtration.slice(12, 14).map(x => (x._2, x._3))
        contains
        (math.sqrt(25.0 + 25.0 + 25.0), Chain(List(11L, 8L, 5L))))
    assert(
      filtration.slice(12, 14).map(x => (x._2, x._3))
        contains
        (math.sqrt(25.0 + 25.0 + 25.0), Chain(List(11L, 9L, 4L))))
    assert(filtration(14) == (14, math.sqrt(25.0 + 25.0 + 25.0), Chain(List(13L, 12L, 10L, 7L))))
  }

  "createFiltration" should "create proper filtration for four points and maxDim=2" in {
    val pointCloud = fourPoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(2))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 14)
    for (i <- 0 to 3) assert(filtration(i) == (i, 0.0, Chain(Empty)))
    assert(filtration(4) == (4, 1.0, Chain(List(1L, 0L))))
    assert(filtration(5) == (5, 2.0, Chain(List(3L, 0L))))
    assert(filtration(6) == (6, math.sqrt(5.0), Chain(List(3L, 1L))))
    assert(filtration(7) == (7, math.sqrt(5.0), Chain(List(6L, 5L, 4L))))
    assert(filtration(8) == (8, math.sqrt(25.0 + 9.0 + 25.0), Chain(List(3L, 2L))))
    assert(filtration(9) == (9, math.sqrt(16.0 + 25.0 + 25.0), Chain(List(2L, 1L))))
    assert(filtration(10) == (10, math.sqrt(16.0 + 25.0 + 25.0), Chain(List(9L, 8L, 6L))))
    assert(filtration(11) == (11, math.sqrt(25.0 + 25.0 + 25.0), Chain(List(2L, 0L))))
    assert(
      filtration.slice(12, 14).map(x => (x._2, x._3))
        contains
        (math.sqrt(25.0 + 25.0 + 25.0), Chain(List(11L, 8L, 5L))))
    assert(
      filtration.slice(12, 14).map(x => (x._2, x._3))
        contains
        (math.sqrt(25.0 + 25.0 + 25.0), Chain(List(11L, 9L, 4L))))
  }

  "createFiltration" should
    "create proper the same filtration for four points with maxDim=None as if maxDim=2" in {
    val pointCloud = fourPoints

    val filtrationMaxDim = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(2))
      .collect()
      .sortBy(_._1)
    val filtrationNoMaxDim = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud)
      .collect()
      .sortBy(_._1)

    assert(filtrationMaxDim.length == filtrationNoMaxDim.length)
    for (i <- 0 to 13) assert(filtrationMaxDim(i) == filtrationNoMaxDim(i))
  }

  "createFiltration" should "create proper filtration for four points and maxDim=1" in {
    val pointCloud = fourPoints

    val filtration = VietorisRipsFiltrationCreator
      .createFiltration(pointCloud, Some(1))
      .collect()
      .sortBy(_._1)

    assert(filtration.length == 10)
    for (i <- 0 to 3) assert(filtration(i) == (i, 0.0, Chain(Empty)))
    assert(filtration(4) == (4, 1.0, Chain(List(1L, 0L))))
    assert(filtration(5) == (5, 2.0, Chain(List(3L, 0L))))
    assert(filtration(6) == (6, math.sqrt(5.0), Chain(List(3L, 1L))))
    assert(filtration(7) == (7, math.sqrt(25.0 + 9.0 + 25.0), Chain(List(3L, 2L))))
    assert(filtration(8) == (8, math.sqrt(16.0 + 25.0 + 25.0), Chain(List(2L, 1L))))
    assert(filtration(9) == (9, math.sqrt(25.0 + 25.0 + 25.0), Chain(List(2L, 0L))))
  }

}
