name := "kafka-data-generator"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2",
  "org.slf4j" % "slf4j-api" % "1.7.36"
)

assembly / mainClass := Some("com.databricks.benchmark.streaming.kafka.KafkaDataGenerator")
assembly / assemblyJarName := "kafka-data-generator.jar"

// Merge strategy for assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}