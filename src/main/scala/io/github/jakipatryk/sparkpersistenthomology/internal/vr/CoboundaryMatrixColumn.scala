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

  implicit val reverseSimplexFiltrationOrdering: Ordering[Simplex] = new Ordering[Simplex] {
    override def compare(x: Simplex, y: Simplex): Int = {
      val radiusCmp = java.lang.Float.compare(y.radius, x.radius)
      if (radiusCmp != 0) radiusCmp
      else java.lang.Long.compare(x.index, y.index)
    }
  }

  val simplexFiltrationOrdering: Ordering[Simplex] = reverseSimplexFiltrationOrdering.reverse

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
    val fullEntries = initialSimplex.getCofacets
      .filterNot(ApparentPairsDetector.isBirthOfApparentPair)
      .toArray
    scala.util.Sorting.quickSort(fullEntries)(simplexFiltrationOrdering)
    CoboundaryMatrixColumn(initialSimplex, fullEntries)
  }

}
