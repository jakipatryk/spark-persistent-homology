## Project overview
This is a Scala + Spark library for computing persistent homology.

It actually computes persistent cohomology, as it is faster and computes the same persistence pairs (one of possible outputs of this library).

It uses "clearing" and "apparent pairs" optimization.

## Project specific software engineering requiraments
- main objectives of this library is performence and correctness, even if it sacrafises code readibility
- the library should only expose small public API, anything that should not be exposed as public API should be in `internal` subpackage
- in `internal` subpackage, prefer to have `private[sparkpersistenthomology]` modifier in top-level entities

## Running tests
To run tests, if `mise` is present on the device (can be checked with `mise --version`):
`mise exec -- sbt test`
else:
`sbt test`

