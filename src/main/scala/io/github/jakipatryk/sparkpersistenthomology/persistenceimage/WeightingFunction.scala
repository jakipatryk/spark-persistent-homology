package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

trait WeightingFunction extends Serializable {

  /** Computes weight of a persistence point.
    */
  def compute(birth: Double, persistence: Double): Double

}

object WeightingFunction {

  /** Weighting function that just takes persistence and ignores birth. */
  object JustPersistenceWeightingFunction extends WeightingFunction {
    override def compute(birth: Double, persistence: Double): Double = persistence
  }

}
