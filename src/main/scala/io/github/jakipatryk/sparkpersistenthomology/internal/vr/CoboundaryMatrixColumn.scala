package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import scala.collection.mutable.PriorityQueue

/** Represents semi-implicitly coboundary matrix column.
  *
  * @param initialSimplex
  *   Simplex which uniquely identifies the column. The column initially is just coboundary of this
  *   simplex.
  * @param simplicesAdded
  *   Array of simplices (actually, their coboundaries) which were added during the matrix
  *   reduction. Initially empty array.
  * @param valueTopEntries
  *   Top entries of the column (coboundary chain), used for best-effort columns addition and pivot
  *   extraction.
  */
private[sparkpersistenthomology] case class CoboundaryMatrixColumn(
  initialSimplex: Simplex,
  simplicesAdded: Array[Simplex],
  valueTopEntries: Array[Simplex]
) {

  @inline def pivot: Long = valueTopEntries(0).index

}

private[sparkpersistenthomology] object CoboundaryMatrixColumn {

  /** Resolves the full initial boundary in the coboundary matrix for a given simplex.
    *
    * @param simplex
    *   The simplex for which to resolve the initial boundary.
    * @return
    *   A max-heap (PriorityQueue) of cofacets ordered first by radius (descending priority), then
    *   by reversed index (ascending priority).
    */
  def resolveInitialBoundary(simplex: Simplex)(implicit
    context: FiltrationContext
  ): PriorityQueue[Simplex] = {
    implicit val ordering: Ordering[Simplex] = Ordering.by(s => (s.radius, -s.index))
    val pq                                   = PriorityQueue.empty[Simplex]
    pq ++= simplex.getCofacets
    pq
  }

}
