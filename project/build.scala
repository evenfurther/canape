import sbt._

object Canape extends Build {

  lazy val canape = Project("canape", file(".")) dependsOn(dispatchLiftJson)

  lazy val dispatchLiftJson = uri("git://github.com/dispatch/dispatch-lift-json#0.1.1")

}
