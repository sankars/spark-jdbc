name := "SparkTableMerger"

version := "1.0"

scalaVersion := "2.11.2"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
	  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
	  "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
