lazy val root = (project in file("."))
  .settings(
    name := "Spark",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
        "org.apache.spark" % "spark-core_2.10" % "1.4.0"
    )
  )
