package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.utils.SimplexIndex

private[sparkpersistenthomology] case class Simplex(index: SimplexIndex, dim: Byte, radius: Float) {

  import Simplex._

  /** Returns an iterator over all facets of this simplex.
    *
    * Facets are returned in ascending order of their indices in the Combinatorial Number System.
    *
    * @return
    *   An iterator of facets (simplices of dimension `dim - 1`).
    */
  def getFacets(implicit context: FiltrationContext): Iterator[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val simplexCombination =
      context.cns.value.getCombinationFromIndex(index.toBigInt, simplexCombinationSize)

    val it = context.cns.value.subcombinationsIndicesIterator(simplexCombination)

    new Iterator[Simplex] {
      override def hasNext: Boolean = it.hasNext

      override def next(): Simplex = {
        val (facetIndex, removedIndex, _) = it.next()
        val maxDistance = computeCombinationRadiusExcept(simplexCombination, removedIndex)
        Simplex(SimplexIndex(facetIndex, context.indexPadding), (dim - 1).toByte, maxDistance)
      }
    }
  }

  /** Returns an iterator over all cofacets of this simplex that have radius within the distance
    * threshold.
    *
    * Cofacets are returned in descending order of their indices in the Combinatorial Number System.
    *
    * @return
    *   An iterator of cofacets (simplices of dimension `dim + 1`).
    */
  def getCofacets(implicit context: FiltrationContext): Iterator[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val simplexCombination =
      context.cns.value.getCombinationFromIndex(index.toBigInt, simplexCombinationSize)

    val validElements = context.distanceMatrix.value.getCommonNeighbors(simplexCombination)
    val it = context.cns.value.supcombinationsIndicesIterator(simplexCombination, validElements)

    new Iterator[Simplex] {
      private var nextSimplex: Simplex = null

      private def advance(): Unit = {
        while (nextSimplex == null && it.hasNext) {
          val (cofacetIndex, addedElement) = it.next()
          val maxDistanceToAddedPoint =
            computeMaxDistanceFromPointWithThreshold(
              addedElement,
              simplexCombination,
              context.distanceThreshold
            )
          val cofacetRadius = math.max(radius, maxDistanceToAddedPoint)

          if (cofacetRadius <= context.distanceThreshold) {
            nextSimplex = Simplex(
              SimplexIndex(cofacetIndex, context.indexPadding),
              (dim + 1).toByte,
              cofacetRadius
            )
          }
        }
      }

      override def hasNext: Boolean = {
        if (nextSimplex == null) advance()
        nextSimplex != null
      }

      override def next(): Simplex = {
        if (!hasNext) throw new java.util.NoSuchElementException()
        val res = nextSimplex
        nextSimplex = null
        res
      }
    }
  }

  def getFirstFacetWithSameRadius(implicit context: FiltrationContext): Option[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val simplexCombination =
      context.cns.value.getCombinationFromIndex(index.toBigInt, simplexCombinationSize)

    val it = context.cns.value.subcombinationsIndicesIterator(simplexCombination)
    var result: Option[Simplex] = None

    while (it.hasNext && result.isEmpty) {
      val (facetIndex, removedIndex, _) = it.next()
      val facetRadius = computeCombinationRadiusExcept(simplexCombination, removedIndex)

      if (facetRadius == radius) {
        result = Some(
          Simplex(SimplexIndex(facetIndex, context.indexPadding), (dim - 1).toByte, facetRadius)
        )
      }
    }

    result
  }

  def getFirstCofacetWithSameRadius(implicit context: FiltrationContext): Option[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val simplexCombination =
      context.cns.value.getCombinationFromIndex(index.toBigInt, simplexCombinationSize)

    val validElements = context.distanceMatrix.value.getCommonNeighbors(simplexCombination)
    val it = context.cns.value.supcombinationsIndicesIterator(simplexCombination, validElements)
    var result: Option[Simplex] = None

    while (it.hasNext && result.isEmpty) {
      val (cofacetIndex, addedElement) = it.next()
      val maxDistanceToAddedPoint =
        computeMaxDistanceFromPointWithThreshold(addedElement, simplexCombination, radius)
      val cofacetRadius = math.max(radius, maxDistanceToAddedPoint)

      if (cofacetRadius == radius) {
        result = Some(
          Simplex(SimplexIndex(cofacetIndex, context.indexPadding), (dim + 1).toByte, cofacetRadius)
        )
      }
    }

    result
  }

}

private[sparkpersistenthomology] object Simplex {

  def apply(index: SimplexIndex, dim: Byte)(implicit context: FiltrationContext): Simplex = {
    val combinationSize = dimToCombinationSize(dim)
    val combination =
      context.cns.value.getCombinationFromIndex(index.toBigInt, combinationSize)
    val radius = computeCombinationRadius(combination)
    Simplex(index, dim, radius)
  }

  def apply(index: BigInt, dim: Byte)(implicit context: FiltrationContext): Simplex = {
    apply(SimplexIndex(index, context.indexPadding), dim)
  }

  @inline def dimToCombinationSize(dim: Byte): Int = dim + 1

  @inline def combinationSizeToDim(combinationSize: Int): Byte = (combinationSize - 1).toByte

  @inline private def getDistance(u: Int, v: Int)(implicit context: FiltrationContext): Float = {
    if (u == v) return 0.0f
    val neighbors = context.distanceMatrix.value.neighbors(u)
    val distances = context.distanceMatrix.value.distances(u)
    var low       = 0
    var high      = neighbors.length - 1
    while (low <= high) {
      val mid    = (low + high) >>> 1
      val midVal = neighbors(mid)
      if (midVal < v) {
        high = mid - 1
      } else if (midVal > v) {
        low = mid + 1
      } else {
        return distances(mid)
      }
    }
    Float.PositiveInfinity
  }

  private[sparkpersistenthomology] def computeCombinationRadiusExcept(
    combination: Array[Int],
    skipIndex: Int
  )(implicit context: FiltrationContext): Float = {
    var maxDistance = 0.0f
    var u           = 0
    while (u < combination.length) {
      if (u != skipIndex) {
        var v = u + 1
        while (v < combination.length) {
          if (v != skipIndex) {
            val dist = getDistance(combination(u), combination(v))
            if (dist > maxDistance) {
              maxDistance = dist
            }
          }
          v += 1
        }
      }
      u += 1
    }
    maxDistance
  }

  private def computeCombinationRadius(
    combination: Array[Int]
  )(implicit context: FiltrationContext): Float = {
    var maxDistance = 0.0f
    var u           = 0
    while (u < combination.length) {
      var v = u + 1
      while (v < combination.length) {
        val dist = getDistance(combination(u), combination(v))
        if (dist > maxDistance) {
          maxDistance = dist
        }
        v += 1
      }
      u += 1
    }
    maxDistance
  }

  private[sparkpersistenthomology] def computeMaxDistanceFromPointWithThreshold(
    pointIndex: Int,
    combination: Array[Int],
    threshold: Float
  )(implicit context: FiltrationContext): Float = {
    var maxDistanceToPoint = 0.0f
    var i                  = 0
    while (i < combination.length) {
      val dist = getDistance(pointIndex, combination(i))
      if (dist > threshold) {
        return Float.PositiveInfinity
      }
      if (dist > maxDistanceToPoint) {
        maxDistanceToPoint = dist
      }
      i += 1
    }
    maxDistanceToPoint
  }

}
