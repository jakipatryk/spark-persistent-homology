# spark-persistent-homology

Implementation of persistent homology computation in Apache Spark.

The goal of this project is to enable persistent homology computation in the context of big data for both topologists and non-topologists. It attempts leverages Spark's distributed computing capabilities to handle larger point clouds and higher-dimensional filtrations.

## Features

- **Distributed Computation:** Uses Apache Spark to distribute the workload of computing filtrations and persistence pairs.
- **Vietoris-Rips Filtration:** Computes persistent homology for Vietoris-Rips complexes
- **Optimized Algorithm:** The algorithm is heavily inspired by [Ripser](https://github.com/Ripser/ripser)
    - **Persistent Cohomology:** Computes persistent cohomology, which is typically faster than homology while yielding the same persistence pairs.
    - **Clearing Optimization:** Efficiently skips redundant column reductions.
    - **Apparent Pairs:** Further optimizes the reduction process by identifying pairs that can be matched without full reduction.
- **Persistence Images:** Support for generating [Persistence Images](https://jmlr.org/papers/v18/16-337.html), a stable vector representation of persistent homology suitable for machine learning.

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

- Bauer, U. (2021). [Ripser: efficient computation of Vietoris–Rips persistence barcodes](https://link.springer.com/article/10.1007/s41468-021-00071-5). Journal of Applied and Computational Topology.
- Adams, H., et al. (2017). [Persistence Images: A Stable Vector Representation of Persistent Homology](https://jmlr.org/papers/v18/16-337.html). Journal of Machine Learning Research.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
