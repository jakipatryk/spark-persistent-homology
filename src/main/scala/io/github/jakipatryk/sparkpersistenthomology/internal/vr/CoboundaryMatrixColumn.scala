package io.github.jakipatryk.sparkpersistenthomology.internal.vr

case class CoboundaryMatrixColumn(
  simplexIndex: Long,
  simplexRadius: Float,
  indicesOfSimplicesColumnsAdded: Array[Long],
  columnTopEntries: Array[Long]
) {

  @inline def pivot: Long = columnTopEntries(0)

}
