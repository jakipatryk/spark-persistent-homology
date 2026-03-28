package io.github.jakipatryk.sparkpersistenthomology.internal.vr

private[sparkpersistenthomology] case class Simplex(index: Long, dim: Byte, radius: Float) {

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
      context.cns.value.getCombinationFromIndex(index, simplexCombinationSize)

    context.cns.value.subcombinationsIterator(simplexCombination).map {
      case (facetIndex, facetCombination, _) =>
        val maxDistance = computeCombinationRadius(facetCombination)
        Simplex(facetIndex, (dim - 1).toByte, maxDistance)
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
      context.cns.value.getCombinationFromIndex(index, simplexCombinationSize)

    context.cns.value.supcombinationsIterator(simplexCombination).flatMap {
      case (cofacetIndex, _, addedElement) =>
        val maxDistanceToAddedPoint = computeMaxDistanceFromPoint(addedElement, simplexCombination)
        val cofacetRadius           = math.max(radius, maxDistanceToAddedPoint)

        if (cofacetRadius <= context.distanceThreshold) {
          Some(Simplex(cofacetIndex, (dim + 1).toByte, cofacetRadius))
        } else {
          None
        }
    }
  }

  private def computeCombinationRadius(
    combination: Array[Int]
  )(implicit context: FiltrationContext): Float = {
    var maxDistance = 0.0f
    var u           = 0
    while (u < combination.length) {
      var v = u + 1
      while (v < combination.length) {
        val dist = context.distanceCalculator.calculateDistance(
          context.pointsCloud.value(combination(u)),
          context.pointsCloud.value(combination(v))
        )
        if (dist > maxDistance) {
          maxDistance = dist
        }
        v += 1
      }
      u += 1
    }
    maxDistance
  }

  private def computeMaxDistanceFromPoint(
    pointIndex: Int,
    combination: Array[Int]
  )(implicit context: FiltrationContext): Float = {
    var maxDistanceToPoint = 0.0f
    var i                  = 0
    while (i < combination.length) {
      val dist = context.distanceCalculator.calculateDistance(
        context.pointsCloud.value(pointIndex),
        context.pointsCloud.value(combination(i))
      )
      if (dist > maxDistanceToPoint) {
        maxDistanceToPoint = dist
      }
      i += 1
    }
    maxDistanceToPoint
  }

}

private[sparkpersistenthomology] object Simplex {

  @inline def dimToCombinationSize(dim: Byte): Int = dim + 1

  @inline def combinationSizeToDim(combinationSize: Int): Byte = (combinationSize - 1).toByte

}
