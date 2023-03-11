package io.github.jakipatryk.sparkpersistenthomology.distances

import io.github.jakipatryk.sparkpersistenthomology.PointsCloud
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class DistanceMatrixSpec extends AnyFlatSpec with BeforeAndAfterAll {

  implicit var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setAppName("PersistenceImageSpec").setMaster("local[*]")
    )
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }


  behavior of "DistanceMatrix.fromPointsCloud"

  it should "return an empty matrix if points cloud is empty" in {
    val emptyRDD = sparkContext.parallelize(Seq.empty[Vector[Double]])
    val emptyPointsCloud = PointsCloud(emptyRDD)

    val distanceMatrix = DistanceMatrix.fromPointsCloud(emptyPointsCloud)

    assert(distanceMatrix.numberOfPoints === 0)
  }

  it should "return a matrix with one element equal 0 if points cloud has one element" in {
    val rdd = sparkContext.parallelize(Seq(Vector(1.0, 2.0, 3.0)))
    val pointsCloud = PointsCloud(rdd)

    val distanceMatrix = DistanceMatrix.fromPointsCloud(pointsCloud)

    assert(distanceMatrix.numberOfPoints === 1)
    assert(distanceMatrix.getDistance(0, 0) === 0.0)
  }

  it should "return a matrix with correct point-wise distances for points cloud with three points" in {
    val rdd = sparkContext.parallelize(
      seq = Seq(Vector(0.0, 0.0, 0.0), Vector(1.0, 2.0, 0.0), Vector(10.0, 0.0, 0.0)),
      numSlices = 1)
    val pointsCloud = PointsCloud(rdd)

    val distanceMatrix = DistanceMatrix.fromPointsCloud(pointsCloud)

    assert(distanceMatrix.numberOfPoints === 3)
    assert(distanceMatrix.getDistance(0, 0) === 0.0)
    assert(distanceMatrix.getDistance(1, 1) === 0.0)
    assert(distanceMatrix.getDistance(2, 2) === 0.0)
    assert(distanceMatrix.getDistance(0, 1) === math.sqrt(5.0))
    assert(distanceMatrix.getDistance(1, 0) === math.sqrt(5.0))
    assert(distanceMatrix.getDistance(0, 2) === 10.0)
    assert(distanceMatrix.getDistance(2, 0) === 10.0)
    assert(distanceMatrix.getDistance(1, 2) === math.sqrt(85.0))
    assert(distanceMatrix.getDistance(2, 1) === math.sqrt(85.0))
  }

}
