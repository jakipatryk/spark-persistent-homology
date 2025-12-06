inThisBuild(List(
  organization := "io.github.jakipatryk",
  homepage := Some(url("https://github.com/jakipatryk/spark-persistent-homology")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "jakipatryk",
      "Bartłomiej Baj",
      "bajbartlomiej997@gmail.com",
      url("https://github.com/jakipatryk")
    )
  )
))

name := "spark-persistent-homology"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.14"

crossScalaVersions := Seq("2.12.14", "2.13.12")

val sparkVersion = System.getProperty("spark.version", "3.3.0")

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

parallelExecution in Test := false