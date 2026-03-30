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
      findFirstWithSameRadius(simplex.getFacets, simplex.radius) match {
        case Some(facet) =>
          findFirstWithSameRadius(facet.getCofacets, facet.radius) match {
            case Some(cofacet) => cofacet.index == simplex.index
            case None          => false
          }
        case None => false
      }
    } else {
      false
    }
  }

  private def isBirthOfApparentPair(
    simplex: Simplex
  )(implicit context: FiltrationContext): Boolean = {
    findFirstWithSameRadius(simplex.getCofacets, simplex.radius) match {
      case Some(cofacet) =>
        findFirstWithSameRadius(cofacet.getFacets, cofacet.radius) match {
          case Some(facet) => facet.index == simplex.index
          case None        => false
        }
      case None => false
    }
  }

  private def findFirstWithSameRadius(
    simplices: Iterator[Simplex],
    targetRadius: Float
  ): Option[Simplex] = {
    simplices.find(_.radius == targetRadius)
  }

}
