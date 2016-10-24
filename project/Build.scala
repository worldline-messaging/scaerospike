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
          "com.aerospike" % "aerospike-client" % "latest.integration",
          "io.netty" % "netty-buffer" % "4.0.23.Final",
          "org.scalatest" %% "scalatest" % (if (scalaVersion.value.startsWith("2.9.")) "1.9.2" else "2.2.0") % "test"
        )
      )
  )
}


object Config {
  val kazanNexus = "Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/"
  lazy val publishToNexus = Seq(
    publishTo <<= (version) { version: String =>
      val nexus = "https://api.bintray.com/maven/worldline-messaging-org/maven/scaerospike"
      if (version.trim.endsWith("SNAPSHOT") || version.trim.endsWith("TAPAD"))
        Some("snapshots" at (nexus ))
      else
        Some("releases" at (nexus ))
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

