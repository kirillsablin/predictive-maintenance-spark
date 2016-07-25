name := "spark-predictive-maintenance"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.2"