package io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators

import org.scalatest.flatspec.AnyFlatSpec

class DefaultOrchestratorSpec extends AnyFlatSpec {
  import Orchestrator._

  behavior of "getPartitionIndexForPivot"

  it should "return correct partition index for step=0 when numberOfPartitions perfectly divides filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(orchestrator.getPartitionIndexForPivot(0, 0) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 4) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 10) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 20) === 2)
    assert(orchestrator.getPartitionIndexForPivot(0, 30) === 3)
    assert(orchestrator.getPartitionIndexForPivot(0, 40) === 4)
    assert(orchestrator.getPartitionIndexForPivot(0, 50) === 5)
    assert(orchestrator.getPartitionIndexForPivot(0, 60) === 6)
    assert(orchestrator.getPartitionIndexForPivot(0, 70) === 7)
    assert(orchestrator.getPartitionIndexForPivot(0, 80) === 8)
    assert(orchestrator.getPartitionIndexForPivot(0, 89) === 8)
    assert(orchestrator.getPartitionIndexForPivot(0, 90) === 9)
    assert(orchestrator.getPartitionIndexForPivot(0, 99) === 9)
  }

  it should "return correct partition index for step=0 when numberOfPartitions doesn't divide filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(3, 10)

    assert(orchestrator.getPartitionIndexForPivot(0, 0) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 1) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 2) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 3) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 4) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 5) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 6) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 7) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 8) === 2)
    assert(orchestrator.getPartitionIndexForPivot(0, 9) === 2)
  }

  it should "be independent of step" in {
    val orchestrator = new DefaultOrchestrator(5, 1000)

    for { step <- 0 until orchestrator.numberOfSteps } {
      assert(orchestrator.getPartitionIndexForPivot(step, 0) === 0)
      assert(orchestrator.getPartitionIndexForPivot(step, 100) === 0)
      assert(orchestrator.getPartitionIndexForPivot(step, 200) === 1)
      assert(orchestrator.getPartitionIndexForPivot(step, 999) === 4)
    }
  }


  behavior of "getPartitionRowBounds"

  it should "return correct row bounds for step=0 when numberOfPartitions perfectly divides filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(
      orchestrator.getPartitionRowBounds(0, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(9L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 1) === Bounds(BoundStartInclusive(10L), BoundEndInclusive(19L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 2) === Bounds(BoundStartInclusive(20L), BoundEndInclusive(29L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 3) === Bounds(BoundStartInclusive(30L), BoundEndInclusive(39L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 4) === Bounds(BoundStartInclusive(40L), BoundEndInclusive(49L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 5) === Bounds(BoundStartInclusive(50L), BoundEndInclusive(59L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 6) === Bounds(BoundStartInclusive(60L), BoundEndInclusive(69L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 7) === Bounds(BoundStartInclusive(70L), BoundEndInclusive(79L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 8) === Bounds(BoundStartInclusive(80L), BoundEndInclusive(89L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 9) === Bounds(BoundStartInclusive(90L), BoundEndInclusive(99L))
    )
  }

  it should "return correct row bounds for step=0 when numberOfPartitions doesn't divide filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(3, 10)

    assert(
      orchestrator.getPartitionRowBounds(0, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(3L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 1) === Bounds(BoundStartInclusive(4L), BoundEndInclusive(7L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 2) === Bounds(BoundStartInclusive(8L), BoundEndInclusive(9L))
    )
  }

  it should "be independent of step" in {
    val orchestrator = new DefaultOrchestrator(5, 1000)

    for { step <- 0 until orchestrator.numberOfSteps } {
      assert(
        orchestrator.getPartitionRowBounds(step, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(199L))
      )
      assert(
        orchestrator.getPartitionRowBounds(step, 1) === Bounds(BoundStartInclusive(200L), BoundEndInclusive(399L))
      )
      assert(
        orchestrator.getPartitionRowBounds(step, 2) === Bounds(BoundStartInclusive(400L), BoundEndInclusive(599L))
      )
      assert(
        orchestrator.getPartitionRowBounds(step, 3) === Bounds(BoundStartInclusive(600L), BoundEndInclusive(799L))
      )
      assert(
        orchestrator.getPartitionRowBounds(step, 4) === Bounds(BoundStartInclusive(800L), BoundEndInclusive(999L))
      )
    }
  }


  behavior of "getPartitionColumnBounds"

  it should "return correct column bounds for step=0 when numberOfPartitions perfectly divides filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(
      orchestrator.getPartitionColumnBounds(0, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(9L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 1) === Bounds(BoundStartInclusive(10L), BoundEndInclusive(19L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 2) === Bounds(BoundStartInclusive(20L), BoundEndInclusive(29L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 3) === Bounds(BoundStartInclusive(30L), BoundEndInclusive(39L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 4) === Bounds(BoundStartInclusive(40L), BoundEndInclusive(49L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 5) === Bounds(BoundStartInclusive(50L), BoundEndInclusive(59L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 6) === Bounds(BoundStartInclusive(60L), BoundEndInclusive(69L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 7) === Bounds(BoundStartInclusive(70L), BoundEndInclusive(79L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 8) === Bounds(BoundStartInclusive(80L), BoundEndInclusive(89L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 9) === Bounds(BoundStartInclusive(90L), BoundEndInclusive(99L))
    )
  }

  it should "return correct columns bounds for step=0 when numberOfPartitions doesn't divide filtrationLength" in {
    val orchestrator = new DefaultOrchestrator(3, 10)

    assert(
      orchestrator.getPartitionColumnBounds(0, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(3L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 1) === Bounds(BoundStartInclusive(4L), BoundEndInclusive(7L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(0, 2) === Bounds(BoundStartInclusive(8L), BoundEndInclusive(9L))
    )
  }

  it should "return correct column bounds for step=2 for partitions up to 7th" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(
      orchestrator.getPartitionColumnBounds(2, 0) === Bounds(BoundStartInclusive(20L), BoundEndInclusive(29L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 1) === Bounds(BoundStartInclusive(30L), BoundEndInclusive(39L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 2) === Bounds(BoundStartInclusive(40L), BoundEndInclusive(49L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 3) === Bounds(BoundStartInclusive(50L), BoundEndInclusive(59L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 4) === Bounds(BoundStartInclusive(60L), BoundEndInclusive(69L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 5) === Bounds(BoundStartInclusive(70L), BoundEndInclusive(79L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 6) === Bounds(BoundStartInclusive(80L), BoundEndInclusive(89L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 7) === Bounds(BoundStartInclusive(90L), BoundEndInclusive(99L))
    )
  }


  behavior of "shouldSkipPartition"

  it should "skip none of partitions on step=0" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(orchestrator.shouldSkipPartition(0, 0) === false)
    assert(orchestrator.shouldSkipPartition(0, 1) === false)
    assert(orchestrator.shouldSkipPartition(0, 2) === false)
    assert(orchestrator.shouldSkipPartition(0, 3) === false)
    assert(orchestrator.shouldSkipPartition(0, 4) === false)
    assert(orchestrator.shouldSkipPartition(0, 5) === false)
    assert(orchestrator.shouldSkipPartition(0, 6) === false)
    assert(orchestrator.shouldSkipPartition(0, 7) === false)
    assert(orchestrator.shouldSkipPartition(0, 8) === false)
    assert(orchestrator.shouldSkipPartition(0, 9) === false)
  }

  it should "skip last two partitions on step=2" in {
    val orchestrator = new DefaultOrchestrator(10, 100)

    assert(orchestrator.shouldSkipPartition(2, 0) === false)
    assert(orchestrator.shouldSkipPartition(2, 1) === false)
    assert(orchestrator.shouldSkipPartition(2, 2) === false)
    assert(orchestrator.shouldSkipPartition(2, 3) === false)
    assert(orchestrator.shouldSkipPartition(2, 4) === false)
    assert(orchestrator.shouldSkipPartition(2, 5) === false)
    assert(orchestrator.shouldSkipPartition(2, 6) === false)
    assert(orchestrator.shouldSkipPartition(2, 7) === false)
    assert(orchestrator.shouldSkipPartition(2, 8) === true)
    assert(orchestrator.shouldSkipPartition(2, 9) === true)
  }

}
