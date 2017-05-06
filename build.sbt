name := "GRECDeduplication"

version := "1.0"

scalaVersion := "2.11.0" +
  ""

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"       %    sparkVersion,
  "org.apache.spark"      %%  "spark-sql"        %    sparkVersion,
  "org.apache.spark"      %%  "spark-graphx"     %    sparkVersion,
  "junit"                 %   "junit"            %    "4.12" % "test",
  "io.krom"               %   "lsh-scala_2.11"   %    "0.1",
  "info.debatty"          %   "java-string-similarity" % "0.23"
)
    