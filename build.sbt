lazy val rootSettings = Seq(
  name := "titanic",
  version := "1.0.0-SNAPSHOT",
  isSnapshot := version.value.endsWith("SNAPSHOT"),
  scalaVersion := "2.11.8",
  resolvers += Resolver.sonatypeRepo("snapshots"),
  libraryDependencies ++= {
    val akkaV = "2.4.7"
    val sparkV = "1.6.1"
    Seq(
      "com.github.fommil.netlib" % "all" % "1.1.2",
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-mllib" % sparkV,
      "com.databricks" %% "spark-csv" % "1.4.0",
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaV,
      "org.scalatest" %% "scalatest" % "2.2.6" % "test"
    )
  },
  mainClass in Compile := Some("com.karasiq.titanic.Main")
)

lazy val root = (project in file("."))
  .settings(rootSettings)