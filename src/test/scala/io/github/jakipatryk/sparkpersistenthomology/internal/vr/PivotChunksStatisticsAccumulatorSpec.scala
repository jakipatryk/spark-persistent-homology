package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator._

class PivotChunksStatisticsAccumulatorSpec extends AnyFlatSpec with Matchers {

  "LocalPivotChunksStatistics" should "initialize array with correct size" in {
    val stats = new LocalPivotChunksStatistics(10L, 25L)
    stats.counts.length shouldBe 3 // 0-9, 10-19, 20-24
  }

  it should "correctly map pivots to chunks" in {
    val stats = new LocalPivotChunksStatistics(10L, 25L)
    stats.addPivot(0L)
    stats.addPivot(9L)
    stats.addPivot(10L)
    stats.addPivot(24L)

    stats.counts(0) shouldBe 2L
    stats.counts(1) shouldBe 1L
    stats.counts(2) shouldBe 1L
  }

  "PivotChunksStatisticsAccumulator" should "be zero initially" in {
    val acc = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    acc.isZero shouldBe true
  }

  it should "not be zero after adding pivot" in {
    val stats = new LocalPivotChunksStatistics(10L, 25L)
    stats.addPivot(5L)
    val acc = new PivotChunksStatisticsAccumulator(stats)
    acc.isZero shouldBe false
  }

  it should "not be zero when hasPivotChanged is true but counts are empty" in {
    val stats = new LocalPivotChunksStatistics(10L, 25L)
    stats.hasPivotChanged = true
    val acc = new PivotChunksStatisticsAccumulator(stats)
    acc.isZero shouldBe false
  }

  it should "correctly reset" in {
    val stats = new LocalPivotChunksStatistics(10L, 25L)
    stats.addPivot(5L)
    stats.hasPivotChanged = true
    val acc = new PivotChunksStatisticsAccumulator(stats)
    acc.reset()
    acc.isZero shouldBe true
    acc.state.counts(0) shouldBe 0L
    acc.state.hasPivotChanged shouldBe false
  }

  it should "correctly add LocalPivotChunksStatistics" in {
    val acc        = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    val statsToAdd = new LocalPivotChunksStatistics(10L, 25L)
    statsToAdd.addPivot(1L)
    statsToAdd.addPivot(11L)
    statsToAdd.hasPivotChanged = true

    acc.add(statsToAdd)

    acc.state.counts(0) shouldBe 1L
    acc.state.counts(1) shouldBe 1L
    acc.state.counts(2) shouldBe 0L
    acc.state.hasPivotChanged shouldBe true
  }

  it should "correctly merge with another Accumulator" in {
    val acc1 = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    acc1.state.addPivot(1L)

    val acc2 = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    acc2.state.addPivot(1L)
    acc2.state.addPivot(11L)
    acc2.state.hasPivotChanged = true

    acc1.merge(acc2)

    acc1.state.counts(0) shouldBe 2L
    acc1.state.counts(1) shouldBe 1L
    acc1.state.counts(2) shouldBe 0L
    acc1.state.hasPivotChanged shouldBe true
  }

  it should "correctly copy without sharing mutable state" in {
    val acc1 = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    acc1.state.addPivot(1L)
    acc1.state.hasPivotChanged = true

    val acc2 = acc1.copy().asInstanceOf[PivotChunksStatisticsAccumulator]
    acc2.state.addPivot(1L)
    acc2.state.hasPivotChanged = false // should not affect acc1

    acc1.state.counts(0) shouldBe 1L
    acc2.state.counts(0) shouldBe 2L
    acc1.state.hasPivotChanged shouldBe true
  }

  it should "produce the correct value with ranges" in {
    val acc = new PivotChunksStatisticsAccumulator(new LocalPivotChunksStatistics(10L, 25L))
    acc.state.addPivot(0L)
    acc.state.addPivot(9L)
    acc.state.addPivot(10L)
    acc.state.addPivot(24L)
    acc.state.hasPivotChanged = true

    val result = acc.value

    result.chunks.length shouldBe 3

    result.chunks(0) shouldBe ChunkStatistics(0L, 9L, 2L)
    result.chunks(1) shouldBe ChunkStatistics(10L, 19L, 1L)
    result.chunks(2) shouldBe ChunkStatistics(20L, 24L, 1L)
    result.hasPivotChanged shouldBe true
  }
}
