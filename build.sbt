name := "akka-online"

version := "0.0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.4.12"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.12"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.4.12"

libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.12"

libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % "2.4.11"

libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21"

libraryDependencies += "com.google.guava" % "guava" % "20.0"

