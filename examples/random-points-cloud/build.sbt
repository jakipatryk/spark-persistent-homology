ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.21"

lazy val root = (project in file("."))
  .settings(
    name := "random-points-cloud"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.8" % "provided"
libraryDependencies += "io.github.jakipatryk" %% "spark-persistent-homology" % "0.1.0-SNAPSHOT"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

Compile / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
run / fork := true
run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)
