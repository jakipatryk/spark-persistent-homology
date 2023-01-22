ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "random-points-cloud"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1" % "provided"
libraryDependencies += "io.github.jakipatryk" %% "spark-persistent-homology" % "1.0.0-SNAPSHOT"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"
