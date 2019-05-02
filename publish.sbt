import sbt._
import sbt.Keys._

useGpg := true

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

ThisBuild / organization := "com.github.oseval.datahub"

ThisBuild / organizationName := "oseval"

ThisBuild / organizationHomepage := Some(url("https://github.com/oseval/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/oseval/datahub"),
    "scm:git@github.com:oseval/datahub.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "oseval",
    name  = "Evgeniy",
    email = "smlinx@gmail.com",
    url   = url("https://github.com/oseval")
  )
)

ThisBuild / description := "Library for client-server data replication."

ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))

ThisBuild / homepage := Some(url("https://github.com/oseval/datahub"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

publishConfiguration := publishConfiguration.value.withOverwrite(true)

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true