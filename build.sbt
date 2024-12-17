ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Tp_1_DMB",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.1",
      "org.apache.spark" %% "spark-sql" % "3.3.1",
      "org.apache.spark" %% "spark-graphx" % "3.3.1"
    )
  )
