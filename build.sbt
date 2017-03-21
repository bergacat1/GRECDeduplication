name := "GRECDeduplication"

version := "1.0"

scalaVersion := "2.11.0" +
  ""

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"       %   sparkVersion,
  "org.apache.spark"      %%  "spark-sql"        %   sparkVersion
)
    