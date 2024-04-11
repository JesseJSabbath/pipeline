name:= "DataPipeline"
version := "0.1"

scalaVersion := "2.11.12"
javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0",
  /*"org.vegas-viz" %% "vegas" % "0.3.11",
  "com.coxautodata" %% "vegalite4s" % "0.4",
  "com.coxautodata" %% "vegalite4s-spark2" % "0.4",
  "org.vegas-viz" %% "vegas-spark" % "0.3.11",*/
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-hive" % "2.3.2",
  "mysql" % "mysql-connector-java" % "8.0.25")
  /*"org.openjfx" % "javafx" % "11" pomOnly(),
  "org.scalafx" %% "scalafx" % "8.0.192-R14")*/
//  "com.lihaoyi" %% "upickle" % "3.0.0",

