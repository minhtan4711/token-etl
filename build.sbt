ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "postgres-to-arango-etl"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0" % "compile"
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"
libraryDependencies += "com.arangodb" % "arangodb-spark-datasource-3.4_2.13" % "1.5.0"
libraryDependencies += "org.web3j" % "core" % "5.0.0"
libraryDependencies += "io.github.cdimascio" % "java-dotenv" % "5.2.2"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "com.lihaoyi" %% "ujson" % "2.0.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.8" % Test
