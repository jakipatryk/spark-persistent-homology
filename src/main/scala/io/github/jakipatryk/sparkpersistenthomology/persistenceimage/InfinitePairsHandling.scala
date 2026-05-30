package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.PersistencePair
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.max

/** Strategy for handling infinite persistence pairs (those with infinite death) in persistence
  * image generation.
  */
sealed trait InfinitePairsHandling {
  def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair]
}

object InfinitePairsHandling {

  /** Ignore pairs with infinite death. */
  case object Drop extends InfinitePairsHandling {
    override def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair] =
      persistencePairs.filter(p => !p.death.isPosInfinity)
  }

  /** Replace infinite death with a fixed user-specified value. */
  case class ReplaceDeathWithConstant(deathValue: Double) extends InfinitePairsHandling {
    override def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair] = {
      import persistencePairs.sparkSession.implicits._
      persistencePairs.map { p =>
        if (p.death.isPosInfinity) p.copy(death = deathValue.toFloat) else p
      }
    }
  }

  /** Replace infinite persistence with a fixed user-specified value (death = birth + constant). */
  case class ReplacePersistenceWithConstant(persistenceValue: Double)
      extends InfinitePairsHandling {
    override def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair] = {
      import persistencePairs.sparkSession.implicits._
      persistencePairs.map { p =>
        if (p.death.isPosInfinity) p.copy(death = p.birth + persistenceValue.toFloat) else p
      }
    }
  }

  /** Replace infinite death with the maximum finite death observed in the dataset plus a margin. */
  case class ReplaceDeathWithMaxFiniteDeathPlusMargin(margin: Double = 1.0)
      extends InfinitePairsHandling {
    override def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair] = {
      import persistencePairs.sparkSession.implicits._
      val finitePairs = persistencePairs.filter(p => !p.death.isPosInfinity)
      val row = finitePairs
        .map(p => p.death.toDouble)
        .agg(max("value"))
        .head()
      val maxFiniteDeath = if (row.isNullAt(0)) 0.0 else row.getDouble(0)
      val newDeath       = (maxFiniteDeath + margin).toFloat
      persistencePairs.map { p =>
        if (p.death.isPosInfinity) p.copy(death = newDeath) else p
      }
    }
  }

  /** Replace infinite persistence with the maximum finite persistence observed in the dataset plus
    * a margin.
    */
  case class ReplacePersistenceWithMaxFinitePersistencePlusMargin(margin: Double = 1.0)
      extends InfinitePairsHandling {
    override def handle(persistencePairs: Dataset[PersistencePair]): Dataset[PersistencePair] = {
      import persistencePairs.sparkSession.implicits._
      val finitePairs = persistencePairs.filter(p => !p.death.isPosInfinity)
      val row = finitePairs
        .map(p => p.persistence.toDouble)
        .agg(max("value"))
        .head()
      val maxFinitePersistence = if (row.isNullAt(0)) 0.0 else row.getDouble(0)
      val marginFloat          = margin.toFloat
      persistencePairs.map { p =>
        if (p.death.isPosInfinity)
          p.copy(death = p.birth + maxFinitePersistence.toFloat + marginFloat)
        else p
      }
    }
  }

}
