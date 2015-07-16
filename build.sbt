name := "canape"

organization := "net.rfc1149"

version := "0.0.7-SNAPSHOT"

scalaVersion := "2.11.7"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.12",
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
  "com.typesafe.play" %% "play-json" % "2.3.9",
  "net.ceedubs" %% "ficus" % "1.1.2",
  "org.specs2" %% "specs2-core" % "2.4.15" % "test"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

fork in Test := true

publishTo := {
  val path = "/home/sam/rfc1149.net/data/ivy2/" + (if (version.value.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases")
  Some(Resolver.ssh("rfc1149 ivy releases", "rfc1149.net", path) as "sam" withPermissions("0644"))
}
