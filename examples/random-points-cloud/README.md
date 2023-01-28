## random-points-cloud
It is an example Spark job showing how to use 
`io.github.jakipatryk.spark-persistent-homology` to compute persistent homology.

By default, it computes persistence pairs for a randomly generated points cloud 
(in which case their count is printed to standard output, just to trigger action),
or it can compute persistence image(and print it to the standard output).

### How to build fat JAR
```
sbt assembly
```

### CLI arguments
- `--numberOfPoints` - number of points in the generated random points cloud, by default `50`
- `--dim` - dimension of points in the generated random points cloud, by default `8`
- `--maxSimplicesDim` - max dimension of simplices in Vietoris-Rips filtration of the generated points cloud, by default `3` (so job will generate homology classes up to dim 3, but dim 3 classes will never die so their death is going to be infinity)
- `--computePersistenceImage` - should the last step of the job be persistence image or just persistence pairs, by default `false`
- `--numberOfPartitions` - number of partitions used to compute persistence pairs, by default None (automatically determined by the library)

### Example of spark-submit
```
spark-submit \
  --class SparkJob \
  --master <master-url> \
  <path-to-fatjar> \
  [optionally-cli-arguments-from-above]
```