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

  /** Resolves the full value of this column (coboundary chain). */
  def resolveFullColumnValue(implicit context: FiltrationContext): Array[Simplex] = {
    import CoboundaryMatrixColumn.simplexOrdering

    val pqs =
      (initialSimplex +: simplicesAdded).map(CoboundaryMatrixColumn.resolveInitialCoboundary)

    val metaPq = PriorityQueue.empty[PriorityQueue[Simplex]](
      Ordering.by((pq: PriorityQueue[Simplex]) => pq.head)
    )
    for (pq <- pqs if pq.nonEmpty) metaPq.enqueue(pq)

    val result = scala.collection.mutable.ArrayBuffer[Simplex]()
    while (metaPq.nonEmpty) {
      val topPq          = metaPq.dequeue()
      val currentElement = topPq.dequeue()
      if (topPq.nonEmpty) metaPq.enqueue(topPq)

      var count = 1
      while (metaPq.nonEmpty && metaPq.head.head == currentElement) {
        val samePq = metaPq.dequeue()
        samePq.dequeue()
        if (samePq.nonEmpty) metaPq.enqueue(samePq)
        count += 1
      }

      if (count % 2 != 0) {
        result.append(currentElement)
      }
    }
    result.toArray
  }

}

private[sparkpersistenthomology] object CoboundaryMatrixColumn {

  implicit val simplexOrdering: Ordering[Simplex] = Ordering.by(s => (s.radius, -s.index))

  /** Resolves the full initial coboundary in the coboundary matrix for a given simplex.
    *
    * @param simplex
    *   The simplex for which to resolve the initial coboundary.
    * @return
    *   A max-heap (PriorityQueue) of cofacets ordered first by radius (descending priority), then
    *   by reversed index (ascending priority).
    */
  def resolveInitialCoboundary(simplex: Simplex)(implicit
    context: FiltrationContext
  ): PriorityQueue[Simplex] = {
    val pq = PriorityQueue.empty[Simplex]
    pq ++= simplex.getCofacets
    pq
  }

}
