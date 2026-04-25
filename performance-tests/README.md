# performance-tests
This project contains performance tests for the `spark-persistent-homology` library, intendent to be run on a real cluster.

## How to build fat JAR
```bash
sbt assembly
```

## Main Classes and Test Cases

All main classes accept the following arguments:
- `--case <case-name>` (Required)
- `--outputPath <path>` (Optional): location to save the resulting persistence pairs as CSV.
- `--pointsCloudOutputPath <path>` (Optional): location to save the generated points cloud as CSV.

### 1. Torus2D
**Main Class**: `Torus2D`
**Test Cases**:
- `4k-no-threshold`: 4000 points, no threshold, max dim 1.
- `100k-threshold`: 100000 points, threshold 0.5, max dim 1.

### 2. Torus3D100D
**Main Class**: `Torus3D100D`
**Test Cases**:
- `no-threshold`: 1000 points, no threshold, max dim 2.
- `100k-threshold`: 100000 points, threshold 0.3, max dim 2.

### 3. Sphere10D100D
**Main Class**: `Sphere10D100D`
**Test Cases**:
- `100k-threshold`: 100000 points, threshold 0.8, max dim 2.

### 4. RipserBenchmark
**Main Class**: `RipserBenchmark`
**Test Cases**:
- `clifford`: Max Dim 2, Threshold 0.15.
- `o3_1024`: Max Dim 3, Threshold 1.8.
- `o3_4096`: Max Dim 3, Threshold 1.4.

The datasets for these cases are bundled in the JAR and loaded automatically.

## Example of spark-submit
```bash
spark-submit \
  --class Torus2D \
  --master <master-url> \
  performance-tests-assembly-0.1.0-SNAPSHOT.jar \
  --case 4k-no-threshold \
  --outputPath s3://my-bucket/results/torus2d_4k

# Ripser Benchmark example (loads from JAR)
spark-submit \
  --class RipserBenchmark \
  --master <master-url> \
  performance-tests-assembly-0.1.0-SNAPSHOT.jar \
  --case clifford \
  --outputPath s3://my-bucket/results/clifford
```
