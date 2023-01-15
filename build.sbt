name := "spark-persistent-homology"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.12.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

parallelExecution in Test := false