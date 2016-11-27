

name := "FirstSparkApplication"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "2.0.1"
//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.3"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"



libraryDependencies += "com.google.code.gson" % "gson" % "2.8.0"



resolvers += Resolver.mavenLocal