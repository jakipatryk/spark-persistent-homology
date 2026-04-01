package io.github.jakipatryk.sparkpersistenthomology.internal.vr

private[sparkpersistenthomology] object ApparentPairsDetector extends Serializable {

  /** Checks if the given simplex is part of a zero apparent pair.
    *
    * @param simplex
    *   The simplex to check.
    * @return
    *   True if the simplex is in a zero apparent pair, false otherwise.
    */
  def isInZeroApparentPair(simplex: Simplex)(implicit context: FiltrationContext): Boolean = {
    isDeathOfApparentPair(simplex) || isBirthOfApparentPair(simplex)
  }

  private def isDeathOfApparentPair(
    simplex: Simplex
  )(implicit context: FiltrationContext): Boolean = {
    if (simplex.dim > 0) {
      findFirstWithSameRadius(simplex.getFacets, simplex.radius)
        .flatMap(facet => findFirstWithSameRadius(facet.getCofacets, facet.radius))
        .map(_.index == simplex.index)
        .getOrElse(false)
    } else {
      false
    }
  }

  private def isBirthOfApparentPair(
    simplex: Simplex
  )(implicit context: FiltrationContext): Boolean = {
    findFirstWithSameRadius(simplex.getCofacets, simplex.radius)
      .flatMap(cofacet => findFirstWithSameRadius(cofacet.getFacets, cofacet.radius))
      .map(_.index == simplex.index)
      .getOrElse(false)
  }

  private def findFirstWithSameRadius(
    simplices: Iterator[Simplex],
    targetRadius: Float
  ): Option[Simplex] = {
    simplices.find(_.radius == targetRadius)
  }

}
