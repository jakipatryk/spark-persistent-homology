package io.github.jakipatryk.sparkpersistenthomology.internal.vr

import org.apache.spark.sql.{ Dataset, SparkSession }

private[sparkpersistenthomology] object CoboundaryMatrixConstructor {

  /** Constructs `Dataset` of [[CoboundaryMatrixColumn]] for columns that need to be reduced.
    *
    * It skips columns that are known to either become zero or be in zero-persistence pair after
    * reduction (clearing optimization and apparent pairs).
    *
    * Due to clearing optimization, all columns that are kept and are reduced to zero represent
    * birth of essential persistence pair (one without death).
    */
  def construct(
    dim: Byte,
    previousDimResult: Option[Dataset[CoboundaryMatrixColumn]] = None
  )(implicit context: FiltrationContext, spark: SparkSession): Dataset[CoboundaryMatrixColumn] = {
    import spark.implicits._

    val range = getValidIndicesForDim(dim)

    val filteredRange = previousDimResult match {
      case Some(prev) =>
        range.except(prev.flatMap(_.pivot.map(_.index).toSeq))
      case None =>
        range
    }

    filteredRange.mapPartitions { iter =>
      iter.flatMap { index =>
        val simplex = Simplex(index, dim)
        val shouldKeepTheSimplex = simplex.radius <= context.distanceThreshold &&
          !ApparentPairsDetector.isInZeroApparentPair(simplex)

        if (shouldKeepTheSimplex) {
          Some(CoboundaryMatrixColumn(simplex))
        } else {
          None
        }
      }
    }
  }

  private def getValidIndicesForDim(
    dim: Byte
  )(implicit context: FiltrationContext, spark: SparkSession): Dataset[SimplexIndex] = {
    import spark.implicits._
    val combinationSize = Simplex.dimToCombinationSize(dim)

    if (dim == 0) {
      val numCombinations = context.cns.value.allCombinationsCount(combinationSize)
      spark.range(numCombinations.toLong).map(i => SimplexIndex(BigInt(i), context.indexPadding))
    } else {
      val numPoints = context.distanceMatrix.value.neighbors.length

      spark.sparkContext
        .parallelize(0 until numPoints)
        .mapPartitions { iter =>
          val cns            = context.cns.value
          val distanceMatrix = context.distanceMatrix.value

          iter
            .flatMap(v => new CliqueIterator(v, combinationSize, distanceMatrix, cns))
            .map(bi => SimplexIndex(bi, context.indexPadding))
        }
        .toDS()
    }
  }

}
