package io.github.jakipatryk.sparkpersistenthomology

/** Represents a persistence pair (birthThreshold, deathThreshold).
  *
  * Infinite persistence is represented by Float.PositiveInfinity.
  */
case class PersistencePair(dim: Int, birth: Float, death: Float) {

  /** True if the pair never dies (infinite persistence). */
  @inline def isInfinite: Boolean = death.isPosInfinity

  /** True if the pair has a finite death time. */
  @inline def isFinite: Boolean = !isInfinite

  /** The persistence (lifetime) of the pair. Returns Float.PositiveInfinity if the pair is
    * infinite.
    */
  @inline def persistence: Float = death - birth

}

object PersistencePair {
  val Infinity: Float = Float.PositiveInfinity
}
