package io.github.jakipatryk.sparkpersistenthomology.internal.vr

private[sparkpersistenthomology] case class Simplex(index: Long, dim: Byte, radius: Float) {

  import Simplex._

  def getFacets(implicit context: FiltrationContext): Iterator[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val facetCombinationSize   = dimToCombinationSize((dim - 1).toByte)
    val simplex                = context.cns.getCombinationFromIndex(index, simplexCombinationSize)
    val numFacets              = simplex.length

    val facets           = new Array[Simplex](numFacets)
    val facetCombination = new Array[Int](facetCombinationSize)

    var i = 0
    while (i < numFacets) {
      // Remove i-th vertex
      var j = 0
      var k = 0
      while (j < numFacets) {
        if (j != i) {
          facetCombination(k) = simplex(j)
          k += 1
        }
        j += 1
      }
      val facetIndex = context.cns.getIndexFromCombination(facetCombination)

      // Calculate radius of this facet
      var maxDistance = 0.0f
      var u           = 0
      while (u < facetCombination.length) {
        var v = u + 1
        while (v < facetCombination.length) {
          val dist = context.distanceCalculator.calculateDistance(
            context.pointsCloud.value(facetCombination(u)),
            context.pointsCloud.value(facetCombination(v))
          )
          if (dist > maxDistance) {
            maxDistance = dist
          }
          v += 1
        }
        u += 1
      }

      facets(i) = Simplex(facetIndex, (dim - 1).toByte, maxDistance)
      i += 1
    }

    facets.sortBy(_.radius).iterator
  }

  def getCofacets(implicit context: FiltrationContext): Iterator[Simplex] = {
    val simplexCombinationSize = dimToCombinationSize(dim)
    val cofacetCombinationSize = dimToCombinationSize((dim + 1).toByte)
    val simplex                = context.cns.getCombinationFromIndex(index, simplexCombinationSize)

    val numPoints = context.pointsCloud.value.length
    val cofacets  = new scala.collection.mutable.ArrayBuffer[Simplex](numPoints - simplex.length)
    val cofacetCombination = new Array[Int](cofacetCombinationSize)

    var v = 0
    while (v < numPoints) {
      // Check if v is in simplex
      var isVInSimplex = false
      var i            = 0
      while (i < simplex.length && !isVInSimplex) {
        if (simplex(i) == v) isVInSimplex = true
        i += 1
      }

      if (!isVInSimplex) {
        // Compute radius of cofacet
        var maxDistanceToV = 0.0f
        i = 0
        while (i < simplex.length) {
          val dist = context.distanceCalculator.calculateDistance(
            context.pointsCloud.value(v),
            context.pointsCloud.value(simplex(i))
          )
          if (dist > maxDistanceToV) {
            maxDistanceToV = dist
          }
          i += 1
        }
        val cofacetRadius = math.max(radius, maxDistanceToV)

        // Insert v into cofacetCombination maintaining descending order
        i = 0
        var j         = 0
        var vInserted = false
        while (i < cofacetCombination.length) {
          if (!vInserted && (j == simplex.length || v > simplex(j))) {
            cofacetCombination(i) = v
            vInserted = true
          } else {
            cofacetCombination(i) = simplex(j)
            j += 1
          }
          i += 1
        }

        val cofacetIndex = context.cns.getIndexFromCombination(cofacetCombination)
        cofacets += Simplex(cofacetIndex, (dim + 1).toByte, cofacetRadius)
      }
      v += 1
    }

    // Sort descending by radius
    cofacets.sortBy(c => -c.radius).iterator
  }

}

private[sparkpersistenthomology] object Simplex {

  @inline def dimToCombinationSize(dim: Byte): Int = dim + 1

  @inline def combinationSizeToDim(combinationSize: Int): Byte = (combinationSize - 1).toByte

}
