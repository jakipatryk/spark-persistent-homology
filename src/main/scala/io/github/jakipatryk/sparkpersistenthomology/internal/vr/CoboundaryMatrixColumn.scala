package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import scala.collection.mutable.ArrayBuffer

/** Represents semi-implicitly coboundary matrix column.
  *
  * @param initialSimplex
  *   Simplex which uniquely identifies the column. The column initially is just coboundary of this
  *   simplex.
  * @param value
  *   Entries of the column (coboundary chain).
  */
private[sparkpersistenthomology] case class CoboundaryMatrixColumn(
  initialSimplex: Simplex,
  value: Array[Simplex]
) {

  @inline def pivot: Option[Simplex] = value.headOption

}

private[sparkpersistenthomology] object CoboundaryMatrixColumn {

  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions.{ coalesce, col, expr, lit }

  implicit val reverseSimplexFiltrationOrdering: Ordering[Simplex] =
    Ordering.by(s => (-s.radius, s.index))

  val reverseColumnsFiltrationOrderingExpressions: Seq[Column] = Seq(
    col("initialSimplex.radius").desc,
    col("initialSimplex.index").asc
  )

  /** Returns a Catalyst expression to extract the pivot index directly from Tungsten binary format.
    */
  def pivotExpression: Column = {
    coalesce(
      expr("try_element_at(value, 1).index"), // 1-based indexing in Spark SQL
      lit(-1L)
    )
  }

  /** Creates a new CoboundaryMatrixColumn from an initial simplex. */
  def apply(
    initialSimplex: Simplex
  )(implicit context: FiltrationContext): CoboundaryMatrixColumn = {
    val fullEntries = initialSimplex.getCofacets.toArray
    scala.util.Sorting.quickSort(fullEntries)(reverseSimplexFiltrationOrdering.reverse)
    CoboundaryMatrixColumn(initialSimplex, fullEntries)
  }

  /** Merges two arrays of Simplices, sorted by `simplexOrdering` descending, modulo 2. */
  private[vr] def addSimplexChains(
    a: Array[Simplex],
    b: Array[Simplex]
  ): Array[Simplex] = {
    val result = ArrayBuffer[Simplex]()
    var i      = 0
    var j      = 0
    while (i < a.length && j < b.length) {
      val cmp = reverseSimplexFiltrationOrdering.compare(a(i), b(j))
      if (cmp > 0) {
        result += a(i)
        i += 1
      } else if (cmp < 0) {
        result += b(j)
        j += 1
      } else {
        // modulo 2, they cancel out
        i += 1
        j += 1
      }
    }
    while (i < a.length) {
      result += a(i)
      i += 1
    }
    while (j < b.length) {
      result += b(j)
      j += 1
    }
    result.toArray
  }

}
