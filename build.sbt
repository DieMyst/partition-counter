name := "partition-counter"

version := "1.0"

scalaVersion := "2.11.7"

val httpAkkaVersion = "1.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-http-experimental_2.11"            % httpAkkaVersion,
  "com.typesafe.akka" % "akka-stream-experimental_2.11"          % httpAkkaVersion,
  "com.typesafe.akka" % "akka-http-core-experimental_2.11"       % httpAkkaVersion,
  "com.typesafe.akka" % "akka-http-spray-json-experimental_2.11" % httpAkkaVersion
)
    