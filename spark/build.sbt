name := "radiant-open-datalake-spark"


scalaVersion := "2.12.18"
scalacOptions ++= Seq("-deprecation")
javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")

// To prevent formatting issues with float values when using Glow. For example, in the Canadian French locale,
// float values are represented with a comma instead of a dot (ex: "3,00" instead of "3.0"). This can lead to
// java number format exceptions when using Glow.
javaOptions ++= Seq(
  "-Duser.language=en",
  "-Duser.country=US"
)

// Library versions
val sparkVersion = "3.5.5"
val glowVersion = "2.0.0"
val datalakeSpark3Version = "14.14.4"
val deltaVersion = "3.1.0"
val scalatestVersion = "3.2.17"

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots",
  "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases"
)

libraryDependencies += "bio.ferlab" %% "datalake-spark3" % datalakeSpark3Version
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % Provided
libraryDependencies += "io.delta" %% "delta-spark" % deltaVersion
libraryDependencies += "io.projectglow" %% "glow-spark3" % glowVersion exclude("org.apache.hadoop", "hadoop-client")

// test dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Test
libraryDependencies += "bio.ferlab" %% "datalake-test-utils" % datalakeSpark3Version % Test


assembly / test := {}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
assembly / assemblyJarName := "radiant-open-datalake-spark.jar"


test / parallelExecution := false
fork := true
