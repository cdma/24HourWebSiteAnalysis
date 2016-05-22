name := "TwentyFourHourWebSiteAnalysis"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"

libraryDependencies += ("org.apache.spark" %% "spark-streaming-kafka" % "1.6.1").exclude("org.spark-project.spark", "unused")

assemblySettings

