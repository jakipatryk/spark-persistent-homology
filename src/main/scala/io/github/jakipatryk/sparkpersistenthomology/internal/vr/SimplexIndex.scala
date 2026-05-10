package io.github.jakipatryk.sparkpersistenthomology.internal.vr

/** Represents a unique index of a simplex in the filtration.
  *
  * Since Spark SQL does not have a native Encoder for `BigInt`, this class wraps a `BigInt` value
  * as a left-padded-by-zeros `String`. This ensures that lexicographical sorting in Spark SQL
  * (e.g., during repartitioning or sorting by index) matches the numerical order of the indices.
  *
  * @param value
  *   The string representation of the index, optionally padded with zeros.
  */
private[sparkpersistenthomology] case class SimplexIndex(value: String)
    extends Ordered[SimplexIndex]
    with Serializable {

  def compare(that: SimplexIndex): Int = this.value.compare(that.value)

  /** Converts the padded string back to a `BigInt`. */
  def toBigInt: BigInt = BigInt(value)

  override def toString: String = value

}

object SimplexIndex {

  import scala.language.implicitConversions

  /** Creates a `SimplexIndex` from a `BigInt` with the specified zero-padding.
    *
    * @param bi
    *   The BigInt index value.
    * @param padding
    *   The total length of the resulting string.
    * @return
    *   A `SimplexIndex` with the padded string.
    */
  def apply(bi: BigInt, padding: Int): SimplexIndex = {
    val s = bi.toString()
    val padded = if (s.length < padding) {
      "0" * (padding - s.length) + s
    } else {
      s
    }
    SimplexIndex(padded)
  }

  /** Creates a `SimplexIndex` from a `Long` with the specified zero-padding. */
  def apply(l: Long, padding: Int): SimplexIndex = apply(BigInt(l), padding)

  implicit def toBigInt(si: SimplexIndex): BigInt = si.toBigInt

  implicit def fromBigInt(bi: BigInt): SimplexIndex = SimplexIndex(bi.toString())

  implicit def fromLong(l: Long): SimplexIndex = SimplexIndex(l.toString)

  implicit def fromInt(i: Int): SimplexIndex = SimplexIndex(i.toString)

}
