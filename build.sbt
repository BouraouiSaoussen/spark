import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Scala Seed Project",
    libraryDependencies += munit % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0",
    libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.8.0",
    libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % "2.8.0" % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-streams-examples" % "2.8.0",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.11",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.14.1",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
    libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2"   
  )
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
