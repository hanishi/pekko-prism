ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "llc.programmer"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val pekkoVersion     = "1.1.3"
val pekkoHttpVersion = "1.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "prism-stream",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream"          % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor-typed"     % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"            % pekkoHttpVersion,
      "ch.qos.logback"    % "logback-classic"       % "1.5.12",
      "org.apache.pekko" %% "pekko-stream-testkit"  % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-http-testkit"    % pekkoHttpVersion % Test,
      "org.scalatest"    %% "scalatest"             % "3.2.19"         % Test
    )
  )

// JMH throughput benchmarks. Depends on the library, but is never published and is
// NOT aggregated by root, so `compile` / `test` / `publish` don't touch it.
// Run it explicitly:  sbt "bench/Jmh/run"
lazy val bench = (project in file("bench"))
  .dependsOn(root)
  .enablePlugins(JmhPlugin)
  .settings(
    name           := "prism-bench",
    publish / skip := true,
    Compile / doc / sources := Seq.empty
  )
