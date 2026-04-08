package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.scalatest.flatspec.AnyFlatSpec
import io.github.jakipatryk.sparkpersistenthomology.SharedSparkContext
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.ChunkStatistics
import io.github.jakipatryk.sparkpersistenthomology.internal.vr.PivotChunksStatisticsAccumulator.PivotChunksStatistics
import org.apache.spark.sql.functions.col

class PartitioningUtilsSpec extends AnyFlatSpec with SharedSparkContext {

  behavior of "getPartitionId"

  it should "map all pivots to 0 when numPartitions is 1" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(ChunkStatistics(0, 9, 100)),
      hasPivotChanged = false
    )
    val df = Seq(0L, 5L, 9L, 100L, -1L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 1))
      .select("part")
      .as[Int]
      .collect()

    assert(result.length == 5)
    assert(result.forall(_ == 0))
  }

  it should "handle empty pivot (-1L) by mapping it to partition 0" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(ChunkStatistics(0, 10, 10), ChunkStatistics(11, 20, 10)),
      hasPivotChanged = false
    )
    val df = Seq(-1L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 2))
      .select("part")
      .as[Int]
      .collect()

    assert(result.head == 0)
  }

  it should "distribute pivots correctly based on balanced chunks" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(
        ChunkStatistics(0, 9, 25),
        ChunkStatistics(10, 19, 25),
        ChunkStatistics(20, 29, 25),
        ChunkStatistics(30, 39, 25)
      ),
      hasPivotChanged = false
    )
    // Total 100, 4 partitions -> target 25.
    // boundaries = [9, 19, 29]
    val df = Seq(5L, 15L, 25L, 35L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 4))
      .select("pivot", "part")
      .as[(Long, Int)]
      .collect()
      .toMap

    assert(result(5L) == 0)
    assert(result(15L) == 1)
    assert(result(25L) == 2)
    assert(result(35L) == 3)
  }

  it should "handle large number of partitions by distincting boundaries" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(ChunkStatistics(0, 9, 10)),
      hasPivotChanged = false
    )
    // 100 partitions, 10 pivots. target = 10 / 100 = 0.
    // boundaries will be filled with 9. distinct -> [9].
    val df = Seq(5L, 15L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 100))
      .select("pivot", "part")
      .as[(Long, Int)]
      .collect()
      .toMap

    assert(result(5L) == 0)
    assert(result(15L) == 1)
  }

  it should "handle chunks with zero counts correctly" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(
        ChunkStatistics(0, 9, 50),
        ChunkStatistics(10, 19, 0),
        ChunkStatistics(20, 29, 50)
      ),
      hasPivotChanged = false
    )
    // Total 100, 2 partitions -> target 50.
    // boundaries = [9]
    val df = Seq(5L, 15L, 25L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 2))
      .select("pivot", "part")
      .as[(Long, Int)]
      .collect()
      .toMap

    assert(result(5L) == 0)
    assert(result(15L) == 1)
    assert(result(25L) == 1)
  }

  it should "work correctly when many chunks fall into one partition" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(
        ChunkStatistics(0, 9, 10),
        ChunkStatistics(10, 19, 10),
        ChunkStatistics(20, 29, 10),
        ChunkStatistics(30, 39, 70)
      ),
      hasPivotChanged = false
    )
    // Total 100, 2 partitions -> target 50.
    // Chunks: 10, 10, 10, 70.
    // C0: 10. next 10. |10-50|=40. |20-50|=30.
    // C1: 20. next 10. |20-50|=30. |30-50|=20.
    // C2: 30. next 70. |30-50|=20. |100-50|=50. -> 20 <= 50. Boundary at 29.
    // boundaries = [29]
    val df = Seq(5L, 15L, 25L, 35L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 2))
      .select("pivot", "part")
      .as[(Long, Int)]
      .collect()
      .toMap

    assert(result(5L) == 0)
    assert(result(15L) == 0)
    assert(result(25L) == 0)
    assert(result(35L) == 1)
  }

  it should "properly handle pivots exactly at boundaries" in {
    import spark.implicits._
    val stats = PivotChunksStatistics(
      Vector(
        ChunkStatistics(0, 10, 50),
        ChunkStatistics(11, 20, 50)
      ),
      hasPivotChanged = false
    )
    // Total 100, 2 partitions -> target 50.
    // chunk 0: currentCount 50. boundaries(0) = 10.
    // boundaries = [10]
    val df = Seq(10L, 11L).toDF("pivot")
    val result = df
      .withColumn("part", PartitioningUtils.getPartitionId(col("pivot"), stats, 2))
      .select("pivot", "part")
      .as[(Long, Int)]
      .collect()
      .toMap

    assert(result(10L) == 0) // binarySearch([10], 10) returns 0.
    assert(result(11L) == 1) // binarySearch([10], 11) returns -2.
  }

}
