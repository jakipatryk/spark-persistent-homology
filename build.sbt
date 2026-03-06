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

scalaVersion := "2.12.21"

scalacOptions ++= Seq("-feature", "-language:postfixOps")

crossScalaVersions := Seq("2.12.21", "2.13.18")

val sparkVersion = System.getProperty("spark.version", "3.5.8")

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

parallelExecution in Test := false

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)
