name := "NYCTaxiAnalysis"

version := "1.0"

scalaVersion := "2.13.16"

// Dépendances Spark 4.0.1 (compatible avec votre installation)
val sparkVersion = "4.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  
  // Pour le logging
  "org.slf4j" % "slf4j-api" % "2.0.9"
)

// Options de compilation pour Scala 2.13
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

// Configuration pour l'exécution
fork := true

javaOptions ++= Seq(
  "-Xms2G",
  "-Xmx8G",
  "-XX:+UseG1GC"
)