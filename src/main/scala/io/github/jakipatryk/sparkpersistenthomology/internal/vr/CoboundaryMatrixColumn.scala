package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ArrayBuffer

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
  valueTopEntries: Array[Simplex],
  isTruncated: Boolean
) {

  @inline def pivot: Option[Simplex] = valueTopEntries.headOption

}

private[sparkpersistenthomology] object CoboundaryMatrixColumn {

  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions.{ coalesce, col, expr, lit }

  final val MinTopEntries: Int = 1000
  final val MaxTopEntries: Int = 10000

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
      expr("try_element_at(valueTopEntries, 1).index"), // 1-based indexing in Spark SQL
      lit(-1L)
    )
  }

  /** Creates a new CoboundaryMatrixColumn from an initial simplex. */
  def apply(
    initialSimplex: Simplex
  )(implicit context: FiltrationContext): CoboundaryMatrixColumn = {
    val fullEntries = initialSimplex.getCofacets.toArray
    scala.util.Sorting.quickSort(fullEntries)(reverseSimplexFiltrationOrdering.reverse)
    val isTruncated = fullEntries.length > MaxTopEntries
    val topEntries  = fullEntries.take(MaxTopEntries)
    CoboundaryMatrixColumn(initialSimplex, Array.empty[Simplex], topEntries, isTruncated)
  }

  /** Returns an unsorted iterator of all cofacets belonging to this column's components. */
  private[vr] def getAllCofacetsIterator(
    initialSimplex: Simplex,
    simplicesAdded: Array[Simplex]
  )(implicit context: FiltrationContext): Iterator[Simplex] = {
    initialSimplex.getCofacets ++ simplicesAdded.iterator.flatMap(_.getCofacets)
  }

  /** Calculates how many elements in a fast sum are "reliable" (not affected by truncation). */
  private[vr] def calculateValidLength(
    fastSum: Array[Simplex],
    thisTruncated: Boolean,
    thisTop: Array[Simplex],
    otherTruncated: Boolean,
    otherTop: Array[Simplex]
  ): Int = {
    var validLength = fastSum.length
    if (thisTruncated) {
      if (thisTop.isEmpty) {
        validLength = 0
      } else {
        val bound = thisTop.last
        val idx   = fastSum.indexWhere(s => reverseSimplexFiltrationOrdering.compare(s, bound) < 0)
        if (idx != -1) validLength = math.min(validLength, idx)
      }
    }
    if (otherTruncated) {
      if (otherTop.isEmpty) {
        validLength = 0
      } else {
        val bound = otherTop.last
        val idx   = fastSum.indexWhere(s => reverseSimplexFiltrationOrdering.compare(s, bound) < 0)
        if (idx != -1) validLength = math.min(validLength, idx)
      }
    }
    validLength
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
