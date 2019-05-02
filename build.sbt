name := "datahub"

ThisBuild / version := "0.3.0-SNAPSHOT"

scalaVersion := "2.12.4"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  Classpaths.sbtPluginReleases
)

val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.specs" % "specs" % "1.4.3" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test"
  )
)

lazy val root = (project in file("."))
  .settings(publish := {}, publishLocal := {}, packagedArtifacts := Map.empty)
  .aggregate(`datahub-core`, `datahub-akka`)

lazy val `datahub-core` = (project in file("datahub-core")).settings(commonSettings)

val akkaVersion = "2.5.21"

lazy val `datahub-akka` = (project in file("datahub-akka"))
  .settings(commonSettings :+ (
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test
    )
  ))
  .dependsOn(`datahub-core`)
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)