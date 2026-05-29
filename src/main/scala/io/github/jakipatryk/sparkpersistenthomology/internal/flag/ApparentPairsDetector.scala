package io.github.jakipatryk.sparkpersistenthomology.internal.flag

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

  /** Finds the birth simplex of an apparent pair, given candidate death simplex.
    *
    * @param death
    *   The candidate death simplex of the apparent pair.
    * @return
    *   Some(birthSimplex) if the given simplex is a death of an apparent pair, None otherwise.
    */
  def getBirthIfIsDeathOfApparentPair(
    death: Simplex
  )(implicit context: FiltrationContext): Option[Simplex] = {
    if (death.dim > 0) {
      death.getFirstFacetWithSameRadius.flatMap { facet =>
        facet.getFirstCofacetWithSameRadius
          .filter(_.index == death.index)
          .map(_ => facet)
      }
    } else {
      None
    }
  }

  def isBirthOfApparentPair(
    simplex: Simplex
  )(implicit context: FiltrationContext): Boolean = {
    simplex.getFirstCofacetWithSameRadius
      .flatMap(cofacet => cofacet.getFirstFacetWithSameRadius)
      .map(_.index == simplex.index)
      .getOrElse(false)
  }

  def isDeathOfApparentPair(
    simplex: Simplex
  )(implicit context: FiltrationContext): Boolean = {
    getBirthIfIsDeathOfApparentPair(simplex).isDefined
  }

}
