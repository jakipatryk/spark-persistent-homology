package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import scala.collection.mutable.{ ArrayBuffer, PriorityQueue }

/** A mutable version of [[CoboundaryMatrixColumn]] used for high-performance local reduction.
  * Adaptive strategy: starts with fast array merges, falls back to a mutable heap if the column
  * grows too large or too many elements cancel out.
  */
private[vr] class MutableCoboundaryMatrixColumn(
  val initialSimplex: Simplex,
  private var simplicesAdded: Array[Simplex],
  private var valueTopEntries: Array[Simplex],
  private var isTruncated: Boolean
)(implicit context: FiltrationContext) {

  private var heap: Option[PriorityQueue[Simplex]] = None

  /** Returns the pivot of the column. Uses fast array pivot if in array-mode, otherwise uses heap
    * pop.
    */
  def pivot: Option[Simplex] = {
    heap match {
      case Some(pq) =>
        while (pq.nonEmpty) {
          val p = pq.dequeue()
          if (pq.nonEmpty && pq.head == p) {
            pq.dequeue()
          } else {
            pq.enqueue(p)
            return Some(p)
          }
        }
        None
      case None =>
        valueTopEntries.headOption
    }
  }

  /** Adds an immutable column to this mutable one.
    */
  def +=(other: CoboundaryMatrixColumn): Unit = {
    simplicesAdded = CoboundaryMatrixColumn.addSimplexChains(
      simplicesAdded,
      CoboundaryMatrixColumn.addSimplexChains(other.simplicesAdded, Array(other.initialSimplex))
    )

    heap match {
      case Some(pq) =>
        // Already in heap mode: try to use other's top entries if full, else re-generate cofacets.
        if (!other.isTruncated) {
          pq ++= other.valueTopEntries
        } else {
          pq ++= CoboundaryMatrixColumn.getAllCofacetsIterator(
            other.initialSimplex,
            other.simplicesAdded
          )
        }
      case None =>
        addTopEntriesAndCheckFallback(other.valueTopEntries, other.isTruncated)
    }
  }

  /** Adds the coboundary of a single simplex to this column.
    */
  def +=(birthSimplex: Simplex): Unit = {
    simplicesAdded = CoboundaryMatrixColumn.addSimplexChains(
      simplicesAdded,
      Array(birthSimplex)
    )

    heap match {
      case Some(pq) =>
        pq ++= birthSimplex.getCofacets
      case None =>
        val otherTop = birthSimplex.getCofacets.toArray
        scala.util.Sorting.quickSort(otherTop)(
          CoboundaryMatrixColumn.reverseSimplexFiltrationOrdering.reverse
        )
        addTopEntriesAndCheckFallback(otherTop, otherIsTruncated = false)
    }
  }

  /** Helper method that performs fast array merge and fallbacks to heap if result is too small.
    */
  private def addTopEntriesAndCheckFallback(
    otherTop: Array[Simplex],
    otherIsTruncated: Boolean
  ): Unit = {
    val fastSum = CoboundaryMatrixColumn.addSimplexChains(valueTopEntries, otherTop)
    val validLength = CoboundaryMatrixColumn.calculateValidLength(
      fastSum,
      this.isTruncated,
      this.valueTopEntries,
      otherIsTruncated,
      otherTop
    )

    if (
      validLength >= CoboundaryMatrixColumn.MinTopEntries || (!this.isTruncated && !otherIsTruncated)
    ) {
      val numToKeep = math.min(validLength, CoboundaryMatrixColumn.MaxTopEntries)
      valueTopEntries = fastSum.take(numToKeep)
      isTruncated = fastSum.length > numToKeep || this.isTruncated || otherIsTruncated
    } else {
      // Switch to heap mode and populate it from the "ground truth" components.
      val pq =
        PriorityQueue.empty[Simplex](CoboundaryMatrixColumn.reverseSimplexFiltrationOrdering)
      pq ++= CoboundaryMatrixColumn.getAllCofacetsIterator(initialSimplex, simplicesAdded)
      heap = Some(pq)
    }
  }

  /** Converts this mutable column back to an immutable [[CoboundaryMatrixColumn]]. Drains the
    * internal heap if present.
    */
  def toImmutableAndDrain: CoboundaryMatrixColumn = {
    heap match {
      case Some(pq) =>
        val finalChain = ArrayBuffer.empty[Simplex]
        while (pq.nonEmpty) {
          val p = pq.dequeue()
          if (pq.nonEmpty && pq.head == p) {
            pq.dequeue()
          } else {
            finalChain += p
          }
        }
        val resolved = finalChain.toArray
        CoboundaryMatrixColumn(
          initialSimplex,
          simplicesAdded,
          resolved.take(CoboundaryMatrixColumn.MaxTopEntries),
          resolved.length > CoboundaryMatrixColumn.MaxTopEntries
        )
      case None =>
        CoboundaryMatrixColumn(initialSimplex, simplicesAdded, valueTopEntries, isTruncated)
    }
  }

}

private[vr] object MutableCoboundaryMatrixColumn {

  def apply(
    col: CoboundaryMatrixColumn
  )(implicit context: FiltrationContext): MutableCoboundaryMatrixColumn = {
    new MutableCoboundaryMatrixColumn(
      col.initialSimplex,
      col.simplicesAdded,
      col.valueTopEntries,
      col.isTruncated
    )
  }

}
