package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import io.github.jakipatryk.sparkpersistenthomology.internal.utils.LazyModulo2SortedArrayPriorityQueue

/** A mutable version of [[CoboundaryMatrixColumn]] used for high-performance local reduction.
  */
private[vr] class MutableCoboundaryMatrixColumn(
  val initialSimplex: Simplex,
  private val queue: LazyModulo2SortedArrayPriorityQueue[Simplex]
)(implicit context: FiltrationContext) {

  /** Returns the pivot of the column.
    */
  def pivot: Option[Simplex] = queue.peek

  /** Adds an immutable column to this mutable one.
    */
  def +=(other: CoboundaryMatrixColumn): Unit = {
    queue += other.value
  }

  /** Adds the coboundary of a single simplex to this column.
    */
  def +=(birthSimplex: Simplex)(implicit context: FiltrationContext): Unit = {
    val cofacets =
      birthSimplex.getCofacets.filterNot(ApparentPairsDetector.isBirthOfApparentPair).toArray
    scala.util.Sorting.quickSort(cofacets)(
      CoboundaryMatrixColumn.simplexFiltrationOrdering
    )
    queue += cofacets
  }

  /** Dequeues the current pivot and adds it to an internal result buffer.
    */
  def dequeueToBuffer(): Unit = queue.dequeueToBuffer()

  def dequeue(): Simplex = queue.dequeue()

  def nonEmpty: Boolean = queue.nonEmpty

  /** Converts this mutable column back to an immutable [[CoboundaryMatrixColumn]]. Drains the
    * column.
    */
  def toImmutableAndDrain: CoboundaryMatrixColumn = {
    CoboundaryMatrixColumn(initialSimplex, queue.drainToArray())
  }

}

private[vr] object MutableCoboundaryMatrixColumn {

  def apply(
    col: CoboundaryMatrixColumn
  )(implicit context: FiltrationContext): MutableCoboundaryMatrixColumn = {
    val queue = new LazyModulo2SortedArrayPriorityQueue[Simplex]()(
      scala.reflect.classTag[Simplex],
      CoboundaryMatrixColumn.reverseSimplexFiltrationOrdering
    )
    queue += col.value
    new MutableCoboundaryMatrixColumn(col.initialSimplex, queue)
  }

}
