ThisBuild / scalaVersion := "2.12.20"

name := "scala-spark-reviews"
version := "0.1.0-SNAPSHOT"

val sparkVersion = "3.5.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

Test / parallelExecution := false
