package io.github.jakipatryk.sparkpersistenthomology.persistenceimage

object PersistenceImageMetrics {

  /** Computes the Euclidean (L2) distance between two Persistence Images. Represents the
    * straight-line distance between the two image matrices.
    */
  def euclideanDistance(image1: PersistenceImage, image2: PersistenceImage): Double = {
    requireComparable(image1, image2)
    val v1  = image1.image.values
    val v2  = image2.image.values
    val len = v1.length
    var sum = 0.0
    var i   = 0
    while (i < len) {
      val diff = v1(i) - v2(i)
      sum += diff * diff
      i += 1
    }
    math.sqrt(sum)
  }

  /** Computes the Manhattan (L1) distance between two Persistence Images. Represents the sum of
    * absolute differences across all pixels.
    */
  def manhattanDistance(image1: PersistenceImage, image2: PersistenceImage): Double = {
    requireComparable(image1, image2)
    val v1  = image1.image.values
    val v2  = image2.image.values
    val len = v1.length
    var sum = 0.0
    var i   = 0
    while (i < len) {
      sum += math.abs(v1(i) - v2(i))
      i += 1
    }
    sum
  }

  /** Computes the Chebyshev (L-infinity) distance between two Persistence Images. Represents the
    * maximum absolute difference across all pixels.
    */
  def chebyshevDistance(image1: PersistenceImage, image2: PersistenceImage): Double = {
    requireComparable(image1, image2)
    val v1      = image1.image.values
    val v2      = image2.image.values
    val len     = v1.length
    var maxDiff = 0.0
    var i       = 0
    while (i < len) {
      val diff = math.abs(v1(i) - v2(i))
      if (diff > maxDiff) maxDiff = diff
      i += 1
    }
    maxDiff
  }

  /** Computes the Cosine Similarity between two Persistence Images. Measures structural/shape
    * similarity independent of magnitude/scale. Returns a value between -1.0 and 1.0. If both
    * images are zero-matrices, returns 1.0. If only one is a zero-matrix, returns 0.0.
    */
  def cosineSimilarity(image1: PersistenceImage, image2: PersistenceImage): Double = {
    requireComparable(image1, image2)
    val v1  = image1.image.values
    val v2  = image2.image.values
    val len = v1.length

    var dotProduct = 0.0
    var norm1Sq    = 0.0
    var norm2Sq    = 0.0
    var i          = 0

    while (i < len) {
      val val1 = v1(i)
      val val2 = v2(i)
      dotProduct += val1 * val2
      norm1Sq += val1 * val1
      norm2Sq += val2 * val2
      i += 1
    }

    if (norm1Sq == 0.0 && norm2Sq == 0.0) {
      1.0
    } else if (norm1Sq == 0.0 || norm2Sq == 0.0) {
      0.0
    } else {
      dotProduct / (math.sqrt(norm1Sq) * math.sqrt(norm2Sq))
    }
  }

  private def requireComparable(image1: PersistenceImage, image2: PersistenceImage): Unit = {
    require(
      image1.birthBound == image2.birthBound,
      s"Birth bounds must match. Got ${image1.birthBound} and ${image2.birthBound}"
    )
    require(
      image1.persistenceBound == image2.persistenceBound,
      s"Persistence bounds must match. Got ${image1.persistenceBound} and ${image2.persistenceBound}"
    )
    require(
      image1.image.numRows == image2.image.numRows && image1.image.numCols == image2.image.numCols,
      s"Image dimensions must match. Got ${image1.image.numRows}x${image1.image.numCols} and ${image2.image.numRows}x${image2.image.numCols}"
    )
  }

}
