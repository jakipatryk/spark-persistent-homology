package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

/** Represents configuration of bounds of an image. If any of them is None, its value is calculated
  * based of min/max values of birth/persistence (death - birth) of persistence pairs.
  */
case class BirthAndPersistenceBoundsConfig(
  minBirth: Option[Double] = None,
  maxBirth: Option[Double] = None,
  minPersistence: Option[Double] = None,
  maxPersistence: Option[Double] = None
)
