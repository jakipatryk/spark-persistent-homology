package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

import io.github.jakipatryk.sparkpersistenthomology.{ PersistencePair, SharedSparkContext }
import org.scalatest.flatspec.AnyFlatSpec

class InfinitePairsHandlingSpec extends AnyFlatSpec with SharedSparkContext {

  import spark.implicits._

  val birthDeathPairs: List[PersistencePair] = List(
    PersistencePair(0, 0.0f, 10.0f),
    PersistencePair(0, 1.0f, 2.3f),
    PersistencePair(0, 0.2f, 18.6f),
    PersistencePair(0, 0.0f, 6.2f),
    PersistencePair(0, 8.0f, 15.0f),
    PersistencePair(0, 0.0f, PersistencePair.Infinity),
    PersistencePair(0, 5.0f, PersistencePair.Infinity)
  )

  behavior of "InfinitePairsHandling"

  it should "drop infinite pairs by default (Drop)" in {
    val ds = spark.createDataset(birthDeathPairs)

    val strategy = InfinitePairsHandling.Drop
    val result   = strategy.handle(ds).collect()

    assert(result.length === 5)
    assert(result.forall(!_.isInfinite))
  }

  it should "replace infinite pairs death with constant" in {
    val ds = spark.createDataset(birthDeathPairs)

    val strategy = InfinitePairsHandling.ReplaceDeathWithConstant(100.0)
    val result   = strategy.handle(ds).collect()

    assert(result.length === 7)
    assert(result.contains(PersistencePair(0, 0.0f, 100.0f)))
    assert(result.contains(PersistencePair(0, 5.0f, 100.0f)))
  }

  it should "replace infinite pairs persistence with constant" in {
    val ds = spark.createDataset(birthDeathPairs)

    val strategy = InfinitePairsHandling.ReplacePersistenceWithConstant(50.0)
    val result   = strategy.handle(ds).collect()

    assert(result.length === 7)
    // The infinite pairs should now have persistence 50.0, thus death = birth + 50.0
    assert(result.contains(PersistencePair(0, 0.0f, 50.0f)))
    assert(result.contains(PersistencePair(0, 5.0f, 55.0f)))
  }

  it should "replace infinite pairs death with max finite death plus margin" in {
    val ds = spark.createDataset(birthDeathPairs)

    val strategy = InfinitePairsHandling.ReplaceDeathWithMaxFiniteDeathPlusMargin(10.0)
    val result   = strategy.handle(ds).collect()

    assert(result.length === 7)
    // Max finite death is 18.6. Max death plus margin is 28.6.
    assert(result.exists(p => p.birth == 0.0f && math.abs(p.death - 28.6) < 1e-5))
    assert(result.exists(p => p.birth == 5.0f && math.abs(p.death - 28.6) < 1e-5))
  }

  it should "replace infinite pairs persistence with max finite persistence plus margin" in {
    val ds = spark.createDataset(birthDeathPairs)

    val strategy = InfinitePairsHandling.ReplacePersistenceWithMaxFinitePersistencePlusMargin(10.0)
    val result   = strategy.handle(ds).collect()

    assert(result.length === 7)
    // Max finite persistence is 18.6 - 0.2 = 18.4
    // Thus new persistence for the infinite pairs is 18.4 + 10.0 = 28.4
    // death = birth + 28.4
    assert(result.exists(p => p.birth == 0.0f && math.abs(p.death - 28.4) < 1e-5))
    assert(result.exists(p => p.birth == 5.0f && math.abs(p.death - 33.4) < 1e-5))
  }

}
