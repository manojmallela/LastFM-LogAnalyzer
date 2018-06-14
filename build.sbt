
organization := "com.manojmallela"

name := "LastFM Log Analyzer"

version := "0.1"

scalaVersion := "2.11.12"

parallelExecution in Test := false


libraryDependencies in ThisBuild ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.3.0_0.9.0" % Test,
  "org.apache.spark" % "spark-hive_2.11" % "2.3.0" % Test,
  "junit" % "junit" % "4.10"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.discard
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}