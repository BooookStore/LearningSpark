name := "learningSpark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "net.sf.opencsv" % "opencsv" % "2.3"
)
