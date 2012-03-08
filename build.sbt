name := "canape"

organization := "net.rfc1149"

version := "0.0.5-SNAPSHOT"

resolvers += "Typesafe repository (releases)" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq("io.netty" % "netty" % "3.3.1.Final",
                            "com.typesafe.akka" % "akka-actor" % "2.0",
			    "net.liftweb" %% "lift-json" % "2.4",
			    "org.specs2" %% "specs2" % "1.8.2" % "test")

scalacOptions ++= Seq("-unchecked", "-deprecation")

publishMavenStyle := true

publishTo <<= (version) { version: String =>
  val path = "/home/sam/rfc1149.net/data/maven2/" +
      (if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases")
  Some(Resolver.sftp("Maven Releases", "rfc1149.net", path) as "sam")
}
