name := "dnotifier"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.2"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  Classpaths.sbtPluginReleases
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.2",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.2" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.specs" % "specs" % "1.4.3",
  "org.mockito" % "mockito-all" % "1.10.19",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0",
  "org.easymock" % "easymock" % "3.4")