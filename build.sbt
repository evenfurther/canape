import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

lazy val canape = project
    .in(file("."))
    .settings(
      name := "canape",
      organization := "net.rfc1149",
      version := "0.0.9-SNAPSHOT",
      scalaVersion := "2.13.10",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % "2.6.20",
        "com.typesafe.akka" %% "akka-stream" % "2.6.20",
        "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.20" % "test",
        "com.typesafe.akka" %% "akka-http" % "10.2.10",
        "de.heikoseeberger" %% "akka-http-play-json" % "1.39.2",
        "com.iheart" %% "ficus" % "1.5.2",
        "org.specs2" %% "specs2-core" % "4.19.0" % "test",
        "org.specs2" %% "specs2-mock" % "4.19.0" % "test"
      ),
      Test/fork := true,
      scalariformAutoformat := true,
      ScalariformKeys.preferences := ScalariformKeys.preferences.value
        .setPreference(AlignArguments, true)
        .setPreference(AlignSingleLineCaseStatements, true)
        .setPreference(DoubleIndentConstructorArguments, true)
        .setPreference(SpacesWithinPatternBinders, false)
        .setPreference(SpacesAroundMultiImports, false))

