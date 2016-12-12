name := "MongoCassandraMigration"

version := "1.0"

lazy val `mongocassandramigration` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

val sparkVersion = "1.6.2"

val cassandraVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion,
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "1.1.0",
  specs2 % Test
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" //to resolve version conflicts with play
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"