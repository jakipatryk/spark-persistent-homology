package io.github.jakipatryk.sparkpersistenthomology.matrixreduction.orchestrators

import org.scalatest.flatspec.AnyFlatSpec

class VietorisRipsOptimisedOrchestratorSpec extends AnyFlatSpec {
  import Orchestrator._

  behavior of "getPartitionIndexForPivot"

  it should "return correct partition index" in {
    val orchestrator = new VietorisRipsOptimisedOrchestrator(10, 100, 4)

    // processing dim 0
    assert(orchestrator.getPartitionIndexForPivot(0, 0) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 1) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 9) === 0)
    assert(orchestrator.getPartitionIndexForPivot(0, 10) === 1)
    assert(orchestrator.getPartitionIndexForPivot(0, 54) === 5)
    assert(orchestrator.getPartitionIndexForPivot(6, 0) === 0)
    assert(orchestrator.getPartitionIndexForPivot(7, 1) === 0)
    assert(orchestrator.getPartitionIndexForPivot(8, 9) === 0)
    assert(orchestrator.getPartitionIndexForPivot(9, 10) === 1)
    assert(orchestrator.getPartitionIndexForPivot(2, 54) === 5)
    assert(orchestrator.getPartitionIndexForPivot(10, 1) === 0)
    assert(orchestrator.getPartitionIndexForPivot(15, 9) === 0)
    assert(orchestrator.getPartitionIndexForPivot(18, 10) === 1)
    assert(orchestrator.getPartitionIndexForPivot(18, 54) === 5)

    // processing dim 1
    assert(orchestrator.getPartitionIndexForPivot(19, 100) === 0)
    assert(orchestrator.getPartitionIndexForPivot(19, 594) === 0)
    assert(orchestrator.getPartitionIndexForPivot(19, 595) === 1)
    assert(orchestrator.getPartitionIndexForPivot(19, 2079) === 3)
    assert(orchestrator.getPartitionIndexForPivot(19, 2080) === 4)
    assert(orchestrator.getPartitionIndexForPivot(19, 5049) === 9)
    assert(orchestrator.getPartitionIndexForPivot(21, 100) === 0)
    assert(orchestrator.getPartitionIndexForPivot(22, 594) === 0)
    assert(orchestrator.getPartitionIndexForPivot(26, 595) === 1)
    assert(orchestrator.getPartitionIndexForPivot(27, 2079) === 3)
    assert(orchestrator.getPartitionIndexForPivot(28, 2080) === 4)
    assert(orchestrator.getPartitionIndexForPivot(29, 5049) === 9)
    assert(orchestrator.getPartitionIndexForPivot(35, 2079) === 3)
    assert(orchestrator.getPartitionIndexForPivot(36, 2080) === 4)
    assert(orchestrator.getPartitionIndexForPivot(37, 5049) === 9)

    // processing dim 2
    assert(orchestrator.getPartitionIndexForPivot(38, 5050) === 0)
    assert(orchestrator.getPartitionIndexForPivot(38, 8321) === 0)
    assert(orchestrator.getPartitionIndexForPivot(38, 21219) === 0)
    assert(orchestrator.getPartitionIndexForPivot(38, 21220) === 1)
    assert(orchestrator.getPartitionIndexForPivot(38, 29220) === 1)
    assert(orchestrator.getPartitionIndexForPivot(38, 37389) === 1)
    assert(orchestrator.getPartitionIndexForPivot(38, 37390) === 2)
    assert(orchestrator.getPartitionIndexForPivot(38, 156749) === 9)
    assert(orchestrator.getPartitionIndexForPivot(38, 166749) === 9)
    assert(orchestrator.getPartitionIndexForPivot(43, 5050) === 0)
    assert(orchestrator.getPartitionIndexForPivot(44, 8321) === 0)
    assert(orchestrator.getPartitionIndexForPivot(45, 21219) === 0)
    assert(orchestrator.getPartitionIndexForPivot(46, 21220) === 1)
    assert(orchestrator.getPartitionIndexForPivot(47, 29220) === 1)
    assert(orchestrator.getPartitionIndexForPivot(48, 37389) === 1)
    assert(orchestrator.getPartitionIndexForPivot(49, 37390) === 2)
    assert(orchestrator.getPartitionIndexForPivot(41, 156749) === 9)
    assert(orchestrator.getPartitionIndexForPivot(42, 166749) === 9)
    assert(orchestrator.getPartitionIndexForPivot(56, 37389) === 1)
    assert(orchestrator.getPartitionIndexForPivot(56, 37390) === 2)
    assert(orchestrator.getPartitionIndexForPivot(56, 156749) === 9)
    assert(orchestrator.getPartitionIndexForPivot(56, 166749) === 9)

    // processing dim 3
    assert(orchestrator.getPartitionIndexForPivot(57, 166750) === 0)
    assert(orchestrator.getPartitionIndexForPivot(57, 558872) === 0)
    assert(orchestrator.getPartitionIndexForPivot(57, 558873) === 1)
    assert(orchestrator.getPartitionIndexForPivot(57, 950995) === 1)
    assert(orchestrator.getPartitionIndexForPivot(57, 950996) === 2)
    assert(orchestrator.getPartitionIndexForPivot(57, 4087974) === 9)
    assert(orchestrator.getPartitionIndexForPivot(61, 166750) === 0)
    assert(orchestrator.getPartitionIndexForPivot(62, 558872) === 0)
    assert(orchestrator.getPartitionIndexForPivot(63, 558873) === 1)
    assert(orchestrator.getPartitionIndexForPivot(64, 950995) === 1)
    assert(orchestrator.getPartitionIndexForPivot(68, 950996) === 2)
    assert(orchestrator.getPartitionIndexForPivot(69, 4087974) === 9)
    assert(orchestrator.getPartitionIndexForPivot(75, 950995) === 1)
    assert(orchestrator.getPartitionIndexForPivot(75, 950996) === 2)
    assert(orchestrator.getPartitionIndexForPivot(75, 4087974) === 9)
  }


  behavior of "getPartitionRowBounds"

  it should "return correct row bounds" in {
    val orchestrator = new VietorisRipsOptimisedOrchestrator(3, 100, 3)

    // processing dim 0
    assert(
      orchestrator.getPartitionRowBounds(0, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(33L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 1) === Bounds(BoundStartInclusive(34L), BoundEndInclusive(67L))
    )
    assert(
      orchestrator.getPartitionRowBounds(0, 2) === Bounds(BoundStartInclusive(68L), BoundEndInclusive(99L))
    )
    assert(
      orchestrator.getPartitionRowBounds(1, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(33L))
    )
    assert(
      orchestrator.getPartitionRowBounds(2, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(33L))
    )
    assert(
      orchestrator.getPartitionRowBounds(2, 1) === Bounds(BoundStartInclusive(34L), BoundEndInclusive(67L))
    )
    assert(
      orchestrator.getPartitionRowBounds(2, 2) === Bounds(BoundStartInclusive(68L), BoundEndInclusive(99L))
    )
    assert(
      orchestrator.getPartitionRowBounds(3, 0) === Bounds(BoundStartInclusive(0L), BoundEndInclusive(33L))
    )
    assert(
      orchestrator.getPartitionRowBounds(4, 1) === Bounds(BoundStartInclusive(34L), BoundEndInclusive(67L))
    )
    assert(
      orchestrator.getPartitionRowBounds(4, 2) === Bounds(BoundStartInclusive(68L), BoundEndInclusive(99L))
    )

    // processing dim 1
    assert(
      orchestrator.getPartitionRowBounds(5, 0) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionRowBounds(6, 1) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionRowBounds(6, 2) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )
    assert(
      orchestrator.getPartitionRowBounds(7, 0) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionRowBounds(7, 1) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionRowBounds(7, 2) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )
    assert(
      orchestrator.getPartitionRowBounds(8, 0) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionRowBounds(9, 1) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionRowBounds(9, 2) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )

    // processing dim 2
    assert(
      orchestrator.getPartitionRowBounds(10, 0) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionRowBounds(11, 1) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionRowBounds(11, 2) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )
    assert(
      orchestrator.getPartitionRowBounds(12, 0) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionRowBounds(12, 1) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionRowBounds(12, 2) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )
    assert(
      orchestrator.getPartitionRowBounds(13, 0) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionRowBounds(14, 1) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionRowBounds(14, 2) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )
  }


  behavior of "getPartitionColumnBounds"

  it should "return correct column bounds" in {
    val orchestrator = new VietorisRipsOptimisedOrchestrator(3, 100, 3)

    // processing dim 0 (so columns representing boundary of dim 1 simplices)
    assert(
      orchestrator.getPartitionColumnBounds(0, 2) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(1, 2) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(1, 1) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 0) === Bounds(BoundStartInclusive(100L), BoundEndInclusive(1749L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 1) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(2, 2) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(3, 0) === Bounds(BoundStartInclusive(1750L), BoundEndInclusive(3399L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(3, 1) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(4, 0) === Bounds(BoundStartInclusive(3400L), BoundEndInclusive(5049L))
    )

    // processing dim 1 (so columns representing boundary of dim 2 simplices)
    assert(
      orchestrator.getPartitionColumnBounds(5, 2) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(6, 2) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(6, 1) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(7, 0) === Bounds(BoundStartInclusive(5050L), BoundEndInclusive(58949L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(7, 1) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(7, 2) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(8, 0) === Bounds(BoundStartInclusive(58950L), BoundEndInclusive(112849L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(8, 1) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(9, 0) === Bounds(BoundStartInclusive(112850L), BoundEndInclusive(166749L))
    )

    // processing dim 2 (so columns representing boundary of dim 3 simplices)
    assert(
      orchestrator.getPartitionColumnBounds(10, 2) === Bounds(BoundStartInclusive(166750L), BoundEndInclusive(1473824L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(11, 2) === Bounds(BoundStartInclusive(1473825L), BoundEndInclusive(2780899L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(11, 1) === Bounds(BoundStartInclusive(166750L), BoundEndInclusive(1473824L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(12, 0) === Bounds(BoundStartInclusive(166750L), BoundEndInclusive(1473824L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(12, 1) === Bounds(BoundStartInclusive(1473825L), BoundEndInclusive(2780899L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(12, 2) === Bounds(BoundStartInclusive(2780900L), BoundEndInclusive(4087974L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(13, 0) === Bounds(BoundStartInclusive(1473825L), BoundEndInclusive(2780899L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(13, 1) === Bounds(BoundStartInclusive(2780900L), BoundEndInclusive(4087974L))
    )
    assert(
      orchestrator.getPartitionColumnBounds(14, 0) === Bounds(BoundStartInclusive(2780900L), BoundEndInclusive(4087974L))
    )
  }


  behavior of "shouldSkipPartition"

  it should "skip only idle partitions" in {
    val orchestrator = new VietorisRipsOptimisedOrchestrator(4, 100, 3)

    // processing dim 0
    assert(orchestrator.shouldSkipPartition(0, 0) === true)
    assert(orchestrator.shouldSkipPartition(0, 1) === true)
    assert(orchestrator.shouldSkipPartition(0, 2) === true)
    assert(orchestrator.shouldSkipPartition(0, 3) === false)
    assert(orchestrator.shouldSkipPartition(1, 0) === true)
    assert(orchestrator.shouldSkipPartition(1, 1) === true)
    assert(orchestrator.shouldSkipPartition(1, 2) === false)
    assert(orchestrator.shouldSkipPartition(1, 3) === false)
    assert(orchestrator.shouldSkipPartition(2, 0) === true)
    assert(orchestrator.shouldSkipPartition(2, 1) === false)
    assert(orchestrator.shouldSkipPartition(2, 2) === false)
    assert(orchestrator.shouldSkipPartition(2, 3) === false)
    assert(orchestrator.shouldSkipPartition(3, 0) === false)
    assert(orchestrator.shouldSkipPartition(3, 1) === false)
    assert(orchestrator.shouldSkipPartition(3, 2) === false)
    assert(orchestrator.shouldSkipPartition(3, 3) === false)
    assert(orchestrator.shouldSkipPartition(4, 0) === false)
    assert(orchestrator.shouldSkipPartition(4, 1) === false)
    assert(orchestrator.shouldSkipPartition(4, 2) === false)
    assert(orchestrator.shouldSkipPartition(4, 3) === true)
    assert(orchestrator.shouldSkipPartition(5, 0) === false)
    assert(orchestrator.shouldSkipPartition(5, 1) === false)
    assert(orchestrator.shouldSkipPartition(5, 2) === true)
    assert(orchestrator.shouldSkipPartition(5, 3) === true)
    assert(orchestrator.shouldSkipPartition(6, 0) === false)
    assert(orchestrator.shouldSkipPartition(6, 1) === true)
    assert(orchestrator.shouldSkipPartition(6, 2) === true)
    assert(orchestrator.shouldSkipPartition(6, 3) === true)

    // processing dim 1
    assert(orchestrator.shouldSkipPartition(7, 0) === true)
    assert(orchestrator.shouldSkipPartition(7, 1) === true)
    assert(orchestrator.shouldSkipPartition(7, 2) === true)
    assert(orchestrator.shouldSkipPartition(7, 3) === false)
    assert(orchestrator.shouldSkipPartition(8, 0) === true)
    assert(orchestrator.shouldSkipPartition(8, 1) === true)
    assert(orchestrator.shouldSkipPartition(8, 2) === false)
    assert(orchestrator.shouldSkipPartition(8, 3) === false)
    assert(orchestrator.shouldSkipPartition(9, 0) === true)
    assert(orchestrator.shouldSkipPartition(9, 1) === false)
    assert(orchestrator.shouldSkipPartition(9, 2) === false)
    assert(orchestrator.shouldSkipPartition(9, 3) === false)
    assert(orchestrator.shouldSkipPartition(10, 0) === false)
    assert(orchestrator.shouldSkipPartition(10, 1) === false)
    assert(orchestrator.shouldSkipPartition(10, 2) === false)
    assert(orchestrator.shouldSkipPartition(10, 3) === false)
    assert(orchestrator.shouldSkipPartition(11, 0) === false)
    assert(orchestrator.shouldSkipPartition(11, 1) === false)
    assert(orchestrator.shouldSkipPartition(11, 2) === false)
    assert(orchestrator.shouldSkipPartition(11, 3) === true)
    assert(orchestrator.shouldSkipPartition(12, 0) === false)
    assert(orchestrator.shouldSkipPartition(12, 1) === false)
    assert(orchestrator.shouldSkipPartition(12, 2) === true)
    assert(orchestrator.shouldSkipPartition(12, 3) === true)
    assert(orchestrator.shouldSkipPartition(13, 0) === false)
    assert(orchestrator.shouldSkipPartition(13, 1) === true)
    assert(orchestrator.shouldSkipPartition(13, 2) === true)
    assert(orchestrator.shouldSkipPartition(13, 3) === true)

    // processing dim 2
    assert(orchestrator.shouldSkipPartition(14, 0) === true)
    assert(orchestrator.shouldSkipPartition(14, 1) === true)
    assert(orchestrator.shouldSkipPartition(14, 2) === true)
    assert(orchestrator.shouldSkipPartition(14, 3) === false)
    assert(orchestrator.shouldSkipPartition(15, 0) === true)
    assert(orchestrator.shouldSkipPartition(15, 1) === true)
    assert(orchestrator.shouldSkipPartition(15, 2) === false)
    assert(orchestrator.shouldSkipPartition(15, 3) === false)
    assert(orchestrator.shouldSkipPartition(16, 0) === true)
    assert(orchestrator.shouldSkipPartition(16, 1) === false)
    assert(orchestrator.shouldSkipPartition(16, 2) === false)
    assert(orchestrator.shouldSkipPartition(16, 3) === false)
    assert(orchestrator.shouldSkipPartition(17, 0) === false)
    assert(orchestrator.shouldSkipPartition(17, 1) === false)
    assert(orchestrator.shouldSkipPartition(17, 2) === false)
    assert(orchestrator.shouldSkipPartition(17, 3) === false)
    assert(orchestrator.shouldSkipPartition(18, 0) === false)
    assert(orchestrator.shouldSkipPartition(18, 1) === false)
    assert(orchestrator.shouldSkipPartition(18, 2) === false)
    assert(orchestrator.shouldSkipPartition(18, 3) === true)
    assert(orchestrator.shouldSkipPartition(19, 0) === false)
    assert(orchestrator.shouldSkipPartition(19, 1) === false)
    assert(orchestrator.shouldSkipPartition(19, 2) === true)
    assert(orchestrator.shouldSkipPartition(19, 3) === true)
    assert(orchestrator.shouldSkipPartition(20, 0) === false)
    assert(orchestrator.shouldSkipPartition(20, 1) === true)
    assert(orchestrator.shouldSkipPartition(20, 2) === true)
    assert(orchestrator.shouldSkipPartition(20, 3) === true)
  }

}
