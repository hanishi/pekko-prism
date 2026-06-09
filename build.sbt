ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "llc.programmer"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val pekkoVersion     = "1.6.0"
val pekkoHttpVersion = "1.3.0"
val zioVersion       = "2.1.14"

// The pure rewriting engine: matchers (Aho-Corasick, BMH, Wu-Manber) and rewriters.
// Depends ONLY on pekko-actor for the `ByteString` data type -- no pekko-stream, no
// pekko-http -- so it can be driven from any streaming runtime (Pekko Streams, ZIO
// Streams, fs2, ...). The `Rewriter` contract is framework-agnostic by design.
lazy val core = (project in file("core"))
  .settings(
    name := "pekko-prism-core",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    libraryDependencies += "org.apache.pekko" %% "pekko-actor" % pekkoVersion
  )

lazy val root = (project in file("."))
  .dependsOn(core)
  .settings(
    name := "pekko-prism",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream"          % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor-typed"     % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"            % pekkoHttpVersion,
      "ch.qos.logback"    % "logback-classic"       % "1.5.34",
      "org.apache.pekko" %% "pekko-stream-testkit"  % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-http-testkit"    % pekkoHttpVersion % Test,
      "org.scalatest"    %% "scalatest"             % "3.2.20"         % Test
    ),
    // `sbt run` and the fat jar both default to the config-driven server.
    Compile / run / mainClass  := Some("prism.ProxyServer"),
    // Fat jar for containers: `sbt assembly` -> target/scala-3.3.4/prism-proxy.jar
    assembly / mainClass       := Some("prism.ProxyServer"),
    assembly / assemblyJarName := "prism-proxy.jar",
    assembly / assemblyMergeStrategy := {
      case p if p.endsWith("module-info.class")  => MergeStrategy.discard
      case "reference.conf"                       => MergeStrategy.concat
      case "application.conf"                     => MergeStrategy.concat
      case PathList("META-INF", "services", _*)   => MergeStrategy.concat
      case PathList("META-INF", _*)               => MergeStrategy.discard
      case _                                       => MergeStrategy.first
    }
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

// Proof that the engine ports: the same `core` driven from ZIO Streams as a `ZPipeline`
// instead of a Pekko `GraphStage`. No HTTP, no pekko-stream -- only `core` (+ ByteString)
// and zio-streams. Run its tests:  sbt "zio/test"
lazy val zio = (project in file("zio"))
  .dependsOn(core)
  .settings(
    name := "pekko-prism-zio",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio-streams" % zioVersion,
      "org.scalatest" %% "scalatest"   % "3.2.20" % Test
    ),
    publish / skip := true
  )
