# spark-persistent-homology

Computation of persistent homology in Apache Spark.

The goal of this project is to enable persistent homology computation in the context of big data for both topologists and non-topologists. It leverages Spark's distributed computing capabilities to handle larger point clouds and higher-dimensional filtrations.

## Features
- **Vietoris-Rips Persistent (Co)Homology:** Computes persistence pairs for Vietoris-Rips filtration. The algorithm is inspired by [Ripser](https://github.com/Ripser/ripser) and [Ripser++](https://github.com/simonzhang00/ripser-plusplus) - it uses apparent pair optimization, clearing optimization, but bunch of things have been changed due to distributed nature of computation in this library, for example locally-exhaustive reduction and compress optimization with apparent pairs have been implemented, and matrix representation is only semi-implicit.  
- **Persistence Images:** Support for generating [Persistence Images](https://jmlr.org/papers/v18/16-337.html), a stable vector representation of persistent homology suitable for machine learning or data drift detection.

## API Usage

### Computing Persistent Homology

The main entry point is `PersistentHomology.computePersistentHomology`.

```scala
import io.github.jakipatryk.sparkpersistenthomology.PersistentHomology
import org.apache.spark.sql.Dataset

val pointsCloud: Dataset[Array[Float]] = ... // Your Spark Dataset of points

val maxDim = 2
val persistencePairsArray = PersistentHomology.computePersistentHomology(
  pointsCloud,
  maxDim = maxDim
)

// persistencePairsArray(i) contains a Dataset[PersistencePair] for dimension i
for (dim <- 0 to maxDim) {
  val pairsForDim = persistencePairsArray(dim)
  println(s"Dimension $dim has ${pairsForDim.count()} persistence pairs.")
}
```

### Generating Persistence Images

You can transform persistence pairs into persistence images for use in downstream tasks.

```scala
import io.github.jakipatryk.sparkpersistenthomology.persistenceimage.{BirthAndPersistenceBoundsConfig, PersistenceImage}

val pairsForDim1 = persistencePairsArray(1)

val computedImage = PersistenceImage.fromPersistencePairsGaussian(
  pairsForDim1,
  BirthAndPersistenceBoundsConfig(),
  numberOfPixelsOnBirthAxis = 100,
  numberOfPixelsOnPersistenceAxis = 100,
  variance = 1.0
)

val imageMatrix = computedImage.image // DenseMatrix from Spark ML
```

### Configuration

The library provides several configuration properties that can be set in the `SparkConf` or via `--conf` flag when submitting a Spark job.

#### Vietoris-Rips Coboundary Matrix Reduction

The coboundary matrix reduction is performed in two phases within a loop. You can tune the number of partitions for each phase to optimize performance:

- `spark.persistenthomology.vr.reducer.explicit.partitions`: Number of partitions for the **Explicit Matrix Exhaustive Reduction** phase. This phase clusters columns with the same pivot into the same partition. It is recommended to set this value **relatively low** (e.g., 10-50) to maximize the effectiveness of local reductions within partitions. (Default: `10`)
- `spark.persistenthomology.vr.reducer.apparent.partitions`: Number of partitions for the **Apparent Pair Shallow Matrix Reduction** phase. This phase performs reductions that are local to each column (using the apparent pairs optimization). It is recommended to set this value **high** to maximize parallelism across the cluster. (Default: `200`)

## Installation

Add the following dependency to your `build.sbt`:

```scala
libraryDependencies += "io.github.jakipatryk" %% "spark-persistent-homology" % "0.1.0"
```

## Running Tests

To run the unit tests, use `sbt`:

```bash
sbt test
```

If you have `mise` installed:

```bash
mise exec -- sbt test
```

## References

- [Ripser: efficient computation of Vietoris–Rips persistence barcodes](https://link.springer.com/article/10.1007/s41468-021-00071-5)
- [Persistence Images: A Stable Vector Representation of Persistent Homology](https://jmlr.org/papers/v18/16-337.html)
- [GPU-Accelerated Computation of Vietoris-Rips Persistence Barcodes](https://arxiv.org/abs/2003.07989)
- [Keeping it sparse: Computing Persistent Homology revisited](https://arxiv.org/abs/2211.09075)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
