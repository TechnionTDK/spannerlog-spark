name := "spark_impl"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

//resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % "provided"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0"


libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" classifier "models-english"

//libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" classifier "models-english-kbp"

libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

//libraryDependencies += "databricks" % "spark-corenlp" % "0.2.0-s_2.11"
