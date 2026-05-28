package io.github.jakipatryk.sparkpersistenthomology.internal.flag

import io.github.jakipatryk.sparkpersistenthomology.FiltrationConfig

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ SparkSession, Dataset }

/** Represents a sparse distance matrix for a point cloud.
  *
  * Stores neighbors and their respective distances for each point that are within a specific
  * threshold. Neighborhoods are stored as arrays sorted in descending order of point indices.
  *
  * @param neighbors
  *   An array of arrays, where each inner array contains indices of neighboring points sorted in
  *   descending order.
  * @param distances
  *   An array of arrays containing distances corresponding to the points in `neighbors`.
  */
private[sparkpersistenthomology] case class SparseDistanceMatrix(
  neighbors: Array[Array[Int]],
  distances: Array[Array[Float]]
) {

  /** Computes the common neighbors of all vertices in the given array.
    *
    * The common neighbors are computed by intersecting the neighborhood arrays of each vertex. This
    * uses an efficient two-pointer approach and returns the result as a new array sorted in
    * descending order.
    *
    * @param vertices
    *   Array of vertex indices whose common neighbors are to be found.
    * @return
    *   A sorted (descending) array of common neighbors.
    */
  def getCommonNeighbors(vertices: Array[Int]): Array[Int] = {
    if (vertices.isEmpty) return Array.empty[Int]
    var currentCandidates = neighbors(vertices(0))
    var i                 = 1
    while (i < vertices.length && currentCandidates.nonEmpty) {
      val vNbs           = neighbors(vertices(i))
      val nextCandidates = new Array[Int](math.min(currentCandidates.length, vNbs.length))

      var idx1      = 0
      var idx2      = 0
      var nextCount = 0
      while (idx1 < currentCandidates.length && idx2 < vNbs.length) {
        val val1 = currentCandidates(idx1)
        val val2 = vNbs(idx2)
        if (val1 == val2) {
          nextCandidates(nextCount) = val1
          nextCount += 1
          idx1 += 1
          idx2 += 1
        } else if (val1 > val2) {
          idx1 += 1
        } else {
          idx2 += 1
        }
      }

      if (nextCount == nextCandidates.length) {
        currentCandidates = nextCandidates
      } else {
        currentCandidates = java.util.Arrays.copyOf(nextCandidates, nextCount)
      }
      i += 1
    }
    currentCandidates
  }

}

private[sparkpersistenthomology] object SparseDistanceMatrix {

  def apply(
    config: FiltrationConfig,
    pointsCloudDS: Dataset[Array[Float]],
    pointsCloud: Broadcast[Array[Array[Float]]]
  )(implicit spark: SparkSession): SparseDistanceMatrix = {
    config match {
      case vr: FiltrationConfig.VietorisRips =>
        createVietorisRipsMatrix(pointsCloudDS, pointsCloud, vr)
      case nn: FiltrationConfig.NearestNeighbors =>
        createNearestNeighborsMatrix(pointsCloud, nn)
    }
  }

  private def createVietorisRipsMatrix(
    pointsCloudDS: Dataset[Array[Float]],
    pointsCloud: Broadcast[Array[Array[Float]]],
    config: FiltrationConfig.VietorisRips
  )(implicit spark: SparkSession): SparseDistanceMatrix = {
    val threshold = config.distanceThreshold.getOrElse(
      EnclosingRadiusCalculator.computeRadius(pointsCloudDS, pointsCloud, config.distanceCalculator)
    )

    val numPoints = pointsCloud.value.length

    val collectedNeighbors = spark.sparkContext
      .parallelize(0 until numPoints)
      .mapPartitions { iter =>
        val points        = pointsCloud.value
        val tempIndices   = new Array[Int](numPoints)
        val tempDistances = new Array[Float](numPoints)

        iter.map { i =>
          val pointI = points(i)
          var count  = 0

          var j = numPoints - 1
          while (j >= 0) {
            if (i != j) {
              val dist = config.distanceCalculator.calculateDistance(pointI, points(j))
              if (dist <= threshold) {
                tempIndices(count) = j
                tempDistances(count) = dist
                count += 1
              }
            }
            j -= 1
          }

          val nbsIndices = new Array[Int](count)
          val nbsDists   = new Array[Float](count)
          System.arraycopy(tempIndices, 0, nbsIndices, 0, count)
          System.arraycopy(tempDistances, 0, nbsDists, 0, count)

          (i, nbsIndices, nbsDists)
        }
      }
      .collect()

    val neighbors = new Array[Array[Int]](numPoints)
    val distances = new Array[Array[Float]](numPoints)

    for ((i, nbsIndices, nbsDists) <- collectedNeighbors) {
      neighbors(i) = nbsIndices
      distances(i) = nbsDists
    }

    SparseDistanceMatrix(neighbors, distances)
  }

  private def createNearestNeighborsMatrix(
    pointsCloud: Broadcast[Array[Array[Float]]],
    config: FiltrationConfig.NearestNeighbors
  )(implicit spark: SparkSession): SparseDistanceMatrix = {
    val numPoints = pointsCloud.value.length

    if (numPoints <= 1 || config.k <= 0) {
      val emptyNeighbors = new Array[Array[Int]](numPoints)
      val emptyDistances = new Array[Array[Float]](numPoints)
      for (i <- 0 until numPoints) {
        emptyNeighbors(i) = Array.empty[Int]
        emptyDistances(i) = Array.empty[Float]
      }
      return SparseDistanceMatrix(emptyNeighbors, emptyDistances)
    }

    val k = math.min(config.k, numPoints - 1)

    val collectedNeighbors = spark.sparkContext
      .parallelize(0 until numPoints)
      .mapPartitions { iter =>
        val points = pointsCloud.value
        iter.map { i =>
          val pointI = points(i)

          // Max-heap to keep the k closest points
          val pq = new java.util.PriorityQueue[(Float, Int)](
            k + 1,
            new java.util.Comparator[(Float, Int)] {
              def compare(a: (Float, Int), b: (Float, Int)): Int =
                java.lang.Float.compare(b._1, a._1)
            }
          )

          var j = 0
          while (j < numPoints) {
            if (i != j) {
              val dist = config.distanceCalculator.calculateDistance(pointI, points(j))
              pq.offer((dist, j))
              if (pq.size() > k) {
                pq.poll()
              }
            }
            j += 1
          }

          val nbs   = new Array[Int](pq.size())
          val dists = new Array[Float](pq.size())
          var idx   = pq.size() - 1
          while (!pq.isEmpty) {
            val item = pq.poll()
            nbs(idx) = item._2
            dists(idx) = item._1
            idx -= 1
          }

          (i, nbs, dists)
        }
      }
      .collect()

    val knnNeighbors = new Array[Set[Int]](numPoints)
    val knnDistances = new Array[Map[Int, Float]](numPoints)

    for ((i, nbs, dists) <- collectedNeighbors) {
      knnNeighbors(i) = nbs.toSet
      knnDistances(i) = nbs.zip(dists).toMap
    }

    val finalNeighbors = new Array[Array[Int]](numPoints)
    val finalDistances = new Array[Array[Float]](numPoints)

    for (i <- 0 until numPoints) {
      val nbs    = knnNeighbors(i)
      val mutual = nbs.filter(j => knnNeighbors(j).contains(i)).toArray.sorted(Ordering.Int.reverse)
      val mutualDists = mutual.map(j => knnDistances(i)(j))
      finalNeighbors(i) = mutual
      finalDistances(i) = mutualDists
    }

    SparseDistanceMatrix(finalNeighbors, finalDistances)
  }
}
