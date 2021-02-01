name := "boston-crimes"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.example")

val sparkVersion = "3.0.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
)