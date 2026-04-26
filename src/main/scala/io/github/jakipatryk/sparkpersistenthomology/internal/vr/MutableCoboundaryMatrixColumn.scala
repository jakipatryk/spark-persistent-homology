package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import scala.collection.mutable.ArrayBuffer

/** A mutable version of [[CoboundaryMatrixColumn]] used for high-performance local reduction.
  */
private[vr] class MutableCoboundaryMatrixColumn(
  val initialSimplex: Simplex,
  private var value: Array[Simplex]
)(implicit context: FiltrationContext) {

  /** Returns the pivot of the column.
    */
  def pivot: Option[Simplex] = value.headOption

  /** Adds an immutable column to this mutable one.
    */
  def +=(other: CoboundaryMatrixColumn): Unit = {
    value = CoboundaryMatrixColumn.addSimplexChains(value, other.value)
  }

  /** Adds the coboundary of a single simplex to this column.
    */
  def +=(birthSimplex: Simplex): Unit = {
    val otherTop = birthSimplex.getCofacets.toArray
    scala.util.Sorting.quickSort(otherTop)(
      CoboundaryMatrixColumn.reverseSimplexFiltrationOrdering.reverse
    )
    value = CoboundaryMatrixColumn.addSimplexChains(value, otherTop)
  }

  /** Converts this mutable column back to an immutable [[CoboundaryMatrixColumn]].
    */
  def toImmutableAndDrain: CoboundaryMatrixColumn = {
    CoboundaryMatrixColumn(initialSimplex, value)
  }

}

private[vr] object MutableCoboundaryMatrixColumn {

  def apply(
    col: CoboundaryMatrixColumn
  )(implicit context: FiltrationContext): MutableCoboundaryMatrixColumn = {
    new MutableCoboundaryMatrixColumn(
      col.initialSimplex,
      col.value
    )
  }

}
