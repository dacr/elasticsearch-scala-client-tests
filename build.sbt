name := "elasticsearch-scala-client-tests"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s"  %% "elastic4s-core"                % "7.3.1",
  "com.sksamuel.elastic4s"  %% "elastic4s-client-esjava"       % "7.3.1",
  "com.sksamuel.elastic4s"  %% "elastic4s-json-json4s"         % "7.3.1",
  "org.codelibs"             % "elasticsearch-cluster-runner"  % "7.3.2.1",

  "org.json4s"              %% "json4s-native"                 % "3.6.7",
  "org.json4s"              %% "json4s-ext"                    % "3.6.7",

  "org.scalatest"           %% "scalatest"                     % "3.0.8",
)

