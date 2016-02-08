import sbt._
import Keys._
import sbtrelease.ReleasePlugin._
import sbtrelease._
import ReleaseStateTransformations._

object Scaerospike extends Build {

  lazy val scaerospike: Project = Project("scaerospike", file("."),
    settings = Config.buildSettings ++
      Seq(
      	organization := "com.tapad.scaerospike"
      ) ++ Seq(libraryDependencies ++=
        Seq(
          "com.aerospike" % "aerospike-client" % "3.1.8",
          "io.netty" % "netty-buffer" % "4.0.23.Final",
          "org.scalatest" %% "scalatest" % (if (scalaVersion.value.startsWith("2.9.")) "1.9.2" else "2.2.0") % "test"
        )
      )
  )
}


object Config {
  val kazanNexus = "Kazan Nexus" at "http://kazan.priv.atos.fr/nexus/content/repositories/releases/"
  lazy val publishToNexus = Seq(
    publishTo <<= (version) { version: String =>
      val nexus = "http://kazan.priv.atos.fr/nexus/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT") || version.trim.endsWith("TAPAD"))
        Some("snapshots" at (nexus + "snapshots/"))
      else
        Some("releases" at (nexus + "releases/"))
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false }
  )
  
  val buildSettings = Defaults.defaultSettings ++ releaseSettings ++ publishToNexus ++ Seq(
    organization := "com.tapad",
    scalaVersion := "2.11.7",
    crossScalaVersions := Seq("2.9.3", "2.10.4", "2.11.7"),
    resolvers += kazanNexus,
    publishArtifact in(Compile, packageDoc) := false
  )
}

