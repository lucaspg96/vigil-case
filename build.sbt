ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val hadoopVersion = "3.2.2"
val sparkVersion = "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "vigil-case",
    libraryDependencies ++= Seq (
      // base spark dependencies
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      // hadoop dependencies to enable S3 filesystem
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,

      // test lib
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
    )
  )
