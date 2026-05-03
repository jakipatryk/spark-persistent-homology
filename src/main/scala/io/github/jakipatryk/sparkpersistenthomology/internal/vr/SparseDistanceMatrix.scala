package io.github.jakipatryk.sparkpersistenthomology.internal.vr

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

  /** Creates a sparse distance matrix from a point cloud.
    *
    * @param pointsCloud
    *   Array of point coordinates.
    * @param distanceCalculator
    *   Strategy to compute distances between points.
    * @param distanceThreshold
    *   Maximum distance for points to be considered neighbors.
    * @return
    *   A SparseDistanceMatrix.
    */
  def apply(
    pointsCloud: Array[Array[Float]],
    distanceCalculator: io.github.jakipatryk.sparkpersistenthomology.distances.DistanceCalculator,
    distanceThreshold: Float
  ): SparseDistanceMatrix = {
    val numPoints = pointsCloud.length
    val neighbors = new Array[Array[Int]](numPoints)
    val distances = new Array[Array[Float]](numPoints)

    val tempIndices   = new Array[Int](numPoints)
    val tempDistances = new Array[Float](numPoints)

    for (i <- 0 until numPoints) {
      val pointI = pointsCloud(i)
      var count  = 0

      var j = numPoints - 1
      while (j >= 0) {
        if (i != j) {
          val dist = distanceCalculator.calculateDistance(pointI, pointsCloud(j))
          if (dist <= distanceThreshold) {
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

      neighbors(i) = nbsIndices
      distances(i) = nbsDists
    }

    SparseDistanceMatrix(neighbors, distances)
  }
}
