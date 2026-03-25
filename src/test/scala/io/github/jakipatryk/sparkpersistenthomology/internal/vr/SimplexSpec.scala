package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.broadcast.Broadcast
import io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator
import io.github.jakipatryk.sparkpersistenthomology.internal.utils.CombinatorialNumberSystem

import scala.reflect.ClassTag

// A dummy implementation of Spark's Broadcast for unit testing
class DummyBroadcast[T: ClassTag](val _value: T) extends Broadcast[T](0L) {
  override def getValue(): T                        = _value
  override def doUnpersist(blocking: Boolean): Unit = ()
  override def doDestroy(blocking: Boolean): Unit   = ()
}

class SimplexSpec extends AnyFlatSpec {

  behavior of "getFacets"

  it should "return facets ordered by radius in ascending order" in {
    val pointsCloud = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f)
    )
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    // We have 4 points. max combination size up to 3
    val cns              = CombinatorialNumberSystem(4, 3)
    val simplexDim: Byte = 2 // max combination size is 3

    implicit val context =
      FiltrationContext(cns, new DummyBroadcast(pointsCloud), distanceCalculator)

    // Let's take simplex with vertices [3, 2, 1] (index 3)
    // Its facets are:
    // [3, 2] (index 5), max distance = 1.0
    // [3, 1] (index 4), max distance = 1.0
    // [2, 1] (index 2), max distance = 1.414...
    // They should be ordered by radius ascending, so [3, 2] and [3, 1] before [2, 1]

    val simplex  = Simplex(index = 3L, dim = simplexDim, radius = 1.4142135f)
    val iterator = simplex.getFacets

    val facets = iterator.toList
    assert(facets.length === 3)
    assert(facets(0).radius <= facets(1).radius)
    assert(facets(1).radius <= facets(2).radius)
    assert(facets.forall(_.dim === 1.toByte))

    // Exact check
    val expectedIndices = Set(2L, 4L, 5L)
    assert(facets.map(_.index).toSet === expectedIndices)
    assert(facets(2).index === 2L) // The one with largest distance
  }

  behavior of "getCofacets"

  it should "return cofacets ordered by radius in descending order" in {
    val distanceCalculator = DistanceCalculator.EuclideanDistanceCalculator

    // Let's add a point 4: Array(10.0f, 10.0f)
    val pointsCloud5 = Array(
      Array(0.0f, 0.0f),
      Array(1.0f, 0.0f),
      Array(0.0f, 1.0f),
      Array(1.0f, 1.0f),
      Array(10.0f, 10.0f)
    )
    val cns              = CombinatorialNumberSystem(5, 3)
    val simplexDim: Byte = 1 // combination size 2

    implicit val context =
      FiltrationContext(cns, new DummyBroadcast(pointsCloud5), distanceCalculator)

    // Simplex [1, 0] (index 0)
    // Cofacets with points 2, 3, 4
    // Point 2 -> [2, 1, 0] (index 0) radius = 1.414...
    // Point 3 -> [3, 1, 0] (index 1) radius = 1.414...
    // Point 4 -> [4, 1, 0] (index 4) radius = max(1.0, dist(4, 0)=14.14..., dist(4, 1)=13.45...) = 14.14...

    val simplex  = Simplex(index = 0L, dim = simplexDim, radius = 1.0f)
    val iterator = simplex.getCofacets

    val cofacets = iterator.toList
    assert(cofacets.length === 3)

    // Check descending order
    assert(cofacets(0).radius >= cofacets(1).radius)
    assert(cofacets(1).radius >= cofacets(2).radius)
    assert(cofacets.forall(_.dim === 2.toByte))

    // Exact check
    val expectedIndices = Set(0L, 1L, 4L)
    assert(cofacets.map(_.index).toSet === expectedIndices)
    assert(cofacets(0).index === 4L) // The one with largest distance
  }
}
