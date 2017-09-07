name := "datahub"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.2"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  Classpaths.sbtPluginReleases
)

val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.specs" % "specs" % "1.4.3" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test",
    "org.easymock" % "easymock" % "3.4" % "test"
  )
)

lazy val root = (project in file(".")).aggregate(core, datahubAkka, examples)

lazy val core = (project in file("core")).settings(commonSettings)

lazy val datahubAkka = (project in file("datahub-akka"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % "2.5.2",
    "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.2" % "test"
  ))

lazy val examples = (project in file("examples"))
  .dependsOn(core, datahubAkka)
  .settings(commonSettings)