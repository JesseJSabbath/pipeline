name:= "DataPipeline"
version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.3.2" % "provided")
