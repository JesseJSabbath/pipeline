name:= "DataPipeline"
version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0",
  "org.vegas-viz" %% "vegas" % "0.3.11",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-hive" % "2.3.2",
  "mysql" % "mysql-connector-java" % "8.0.25")
//  "com.lihaoyi" %% "upickle" % "3.0.0",