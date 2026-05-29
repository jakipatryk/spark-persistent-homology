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

  private[flag] case class KnnNode(vertex: Int, neighbors: Array[Int], distances: Array[Float])
  private[flag] case class UndirectedEdge(node1: Int, node2: Int)
  private[flag] case class EdgeOccurrence(count: Int, distance: Float)
  private[flag] case class DirectedEdge(to: Int, distance: Float)

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
      return createEmptyMatrix(numPoints)
    }

    val k = math.min(config.k, numPoints - 1)

    val asymmetricKnn      = computeAsymmetricKnn(pointsCloud, config, numPoints, k)
    val mutualKnn          = computeMutualKnnEdges(asymmetricKnn)
    val collectedNeighbors = mutualKnn.collect()

    assembleSparseMatrix(collectedNeighbors, numPoints)
  }

  private def createEmptyMatrix(numPoints: Int): SparseDistanceMatrix = {
    val emptyNeighbors = new Array[Array[Int]](numPoints)
    val emptyDistances = new Array[Array[Float]](numPoints)
    for (i <- 0 until numPoints) {
      emptyNeighbors(i) = Array.empty[Int]
      emptyDistances(i) = Array.empty[Float]
    }
    SparseDistanceMatrix(emptyNeighbors, emptyDistances)
  }

  private def computeAsymmetricKnn(
    pointsCloud: Broadcast[Array[Array[Float]]],
    config: FiltrationConfig.NearestNeighbors,
    numPoints: Int,
    k: Int
  )(implicit spark: SparkSession): Dataset[KnnNode] = {
    import spark.implicits._
    spark
      .createDataset(0 until numPoints)
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

          KnnNode(i, nbs, dists)
        }
      }
  }

  private def computeMutualKnnEdges(
    asymmetricKnn: Dataset[KnnNode]
  )(implicit spark: SparkSession): Dataset[KnnNode] = {
    import spark.implicits._
    asymmetricKnn.flatMap { knnNode =>
      val res = new Array[(UndirectedEdge, EdgeOccurrence)](knnNode.neighbors.length)
      var idx = 0
      while (idx < knnNode.neighbors.length) {
        val j       = knnNode.neighbors(idx)
        val d       = knnNode.distances(idx)
        val minNode = if (knnNode.vertex < j) knnNode.vertex else j
        val maxNode = if (knnNode.vertex > j) knnNode.vertex else j
        res(idx) = (UndirectedEdge(minNode, maxNode), EdgeOccurrence(1, d))
        idx += 1
      }
      res
    }
      .groupByKey(_._1)
      .reduceGroups { (a: (UndirectedEdge, EdgeOccurrence), b: (UndirectedEdge, EdgeOccurrence)) =>
        (a._1, EdgeOccurrence(a._2.count + b._2.count, a._2.distance))
      }
      .map(_._2)
      .filter(_._2.count == 2)
      .flatMap { case (edge, occurrence) =>
        Iterator(
          (edge.node1, DirectedEdge(edge.node2, occurrence.distance)),
          (edge.node2, DirectedEdge(edge.node1, occurrence.distance))
        )
      }
      .groupByKey(_._1)
      .mapGroups { (vertex: Int, iter: Iterator[(Int, DirectedEdge)]) =>
        val neighbors       = iter.map(_._2).toArray
        val sortedNeighbors = neighbors.sortBy(-_.to)
        val nbs             = sortedNeighbors.map(_.to)
        val dists           = sortedNeighbors.map(_.distance)
        KnnNode(vertex, nbs, dists)
      }
  }

  private def assembleSparseMatrix(
    collectedNeighbors: Array[KnnNode],
    numPoints: Int
  ): SparseDistanceMatrix = {
    val finalNeighbors = new Array[Array[Int]](numPoints)
    val finalDistances = new Array[Array[Float]](numPoints)

    for (i <- 0 until numPoints) {
      finalNeighbors(i) = Array.empty[Int]
      finalDistances(i) = Array.empty[Float]
    }

    for (knnNode <- collectedNeighbors) {
      finalNeighbors(knnNode.vertex) = knnNode.neighbors
      finalDistances(knnNode.vertex) = knnNode.distances
    }

    SparseDistanceMatrix(finalNeighbors, finalDistances)
  }
}
