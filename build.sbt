name := "canape"

organization := "net.rfc1149"

version := "0.0.3-SNAPSHOT"

resolvers ++= Seq("Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots",
                  "Scala-Tools Maven2 Releases Repository" at "http://scala-tools.org/repo-releases")

libraryDependencies ++= Seq("net.databinder" %% "dispatch-json" % "0.8.6" % "compile",
                            "net.databinder" %% "dispatch-http-json" % "0.8.6" % "compile",
                            "net.databinder" %% "dispatch-http" % "0.8.6" % "compile",
                            "net.debasishg" %% "sjson" % "0.15" % "compile",
                            "org.specs2" %% "specs2" % "1.6.1")
