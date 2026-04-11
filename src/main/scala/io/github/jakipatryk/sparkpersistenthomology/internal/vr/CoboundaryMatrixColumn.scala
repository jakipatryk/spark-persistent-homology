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
  valueTopEntries: Array[Simplex]
) {

  @inline def pivot: Option[Simplex] = valueTopEntries.headOption

  /** Adds two CoboundaryMatrixColumn using fast addition on `valueTopEntries`. Falls back to full
    * resolution if the fast addition results in too few entries.
    *
    * NOTE: This operation is NOT commutative, as the `initialSimplex` of the result is always taken
    * from the left-hand side operand.
    *
    * @example
    *   {{{
    *   val result = colA + colB
    *   assert(result.initialSimplex == colA.initialSimplex)
    *   }}}
    */
  def +(
    other: CoboundaryMatrixColumn
  )(implicit context: FiltrationContext): CoboundaryMatrixColumn = {
    import CoboundaryMatrixColumn._

    val mergedSimplicesAdded = addSimplexChains(
      this.simplicesAdded,
      addSimplexChains(other.simplicesAdded, Array(other.initialSimplex))
    )

    val fastSum = addSimplexChains(this.valueTopEntries, other.valueTopEntries)

    if (fastSum.length >= MinTopEntries) {
      CoboundaryMatrixColumn(this.initialSimplex, mergedSimplicesAdded, fastSum.take(MaxTopEntries))
    } else {
      val thisFull  = this.resolveFullColumnValue
      val otherFull = other.resolveFullColumnValue
      val fullSum   = addSimplexChains(thisFull, otherFull)
      CoboundaryMatrixColumn(this.initialSimplex, mergedSimplicesAdded, fullSum.take(MaxTopEntries))
    }
  }

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

  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions.{ coalesce, col, expr, lit }

  final val MinTopEntries: Int = 5
  final val MaxTopEntries: Int = 100

  implicit val simplexOrdering: Ordering[Simplex] = Ordering.by(s => (s.radius, -s.index))

  /** Spark SQL column expressions that sort columns by -simplexOrdering of their initialSimplex. */
  val matrixColumnsOrderingExpressions: Seq[Column] = Seq(
    col("initialSimplex.radius").desc,
    col("initialSimplex.index").asc
  )

  /** Returns a Catalyst expression to extract the pivot index directly from Tungsten binary format.
    *
    * This avoids the serialization/deserialization overhead of extracting the pivot via `map` on
    * the Dataset. The pivot is the `index` of the first element in the `valueTopEntries` array. If
    * the array is empty, it returns -1L.
    */
  def pivotExpression: Column = {
    coalesce(
      expr("try_element_at(valueTopEntries, 1).index"), // 1-based indexing in Spark SQL
      lit(-1L)
    )
  }

  /** Creates a new CoboundaryMatrixColumn from an initial simplex.
    *
    * @param initialSimplex
    *   The simplex to use as the initial simplex of the column.
    */
  def apply(
    initialSimplex: Simplex
  )(implicit context: FiltrationContext): CoboundaryMatrixColumn = {
    val initialCoboundary = resolveInitialCoboundary(initialSimplex)
    val n                 = math.min(initialCoboundary.size, MaxTopEntries)
    val topEntries        = Iterator.continually(initialCoboundary.dequeue()).take(n).toArray
    CoboundaryMatrixColumn(initialSimplex, Array.empty[Simplex], topEntries)
  }

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

  /** Merges two arrays of Simplices, sorted by `simplexOrdering` descending, modulo 2. */
  private def addSimplexChains(
    a: Array[Simplex],
    b: Array[Simplex]
  ): Array[Simplex] = {
    val result = ArrayBuffer[Simplex]()
    var i      = 0
    var j      = 0
    while (i < a.length && j < b.length) {
      val cmp = simplexOrdering.compare(a(i), b(j))
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
