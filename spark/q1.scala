/*
 * Copyright 2026 Databricks, Inc.
 *
 * This work (the "Licensed Material") is licensed under the Creative Commons
 * Attribution-NonCommercial-NoDerivatives 4.0 International License. You may
 * not use this file except in compliance with the License.
 *
 * To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0/
 *
 * Unless required by applicable law or agreed to in writing, the Licensed Material is offered
 * on an "AS-IS" and "AS-AVAILABLE" BASIS, WITHOUT REPRESENTATIONS OR WARRANTIES OF ANY KIND,
 * whether express, implied, statutory, or other. This includes, without limitation, warranties
 * of title, merchantability, fitness for a particular purpose, non-infringement, absence of
 * latent or other defects, accuracy, or the presence or absence of errors, whether or not known
 * or discoverable. To the extent legally permissible, in no event will the Licensor be liable
 * to You on any legal theory (including, without limitation, negligence) or otherwise for
 * any direct, special, indirect, incidental, consequential, punitive, exemplary, or other
 * losses, costs, expenses, or damages arising out of this License or use of the Licensed
 * Material, even if the Licensor has been advised of the possibility of such losses, costs,
 * expenses, or damages.
 */

// Query A (stateless) / Query B (stateful): Kafka-to-Kafka passthrough benchmark.
// Reads from Kafka, truncates payload to 500KB, optionally applies a GroupBy+Count,
// base64-encodes, and writes back to Kafka. Toggle isStateful to switch between queries.

import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._


val isStateful = false
val kafkaBootstrapServers = sys.env("KAFKA_BOOTSTRAP_SERVERS")
val inputTopic = sys.env("INPUT_TOPIC")
val outputTopic = sys.env("OUTPUT_TOPIC")
val checkpointLocation = sys.env("CHECKPOINT_LOCATION")


if (isStateful) {
 // Reduce shuffle buffer wait time to lower latency at the cost of throughput
 spark.conf.set("spark.shuffle.streaming.networkBufferMaxWaitTimeMs", 10)
 spark.conf.set("spark.sql.shuffle.partitions", 40)
 spark.conf.set("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows", false)
 spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", true)
}


val kafkaStream = spark.readStream
 .format("kafka")
 .option("kafka.bootstrap.servers", kafkaBootstrapServers)
 .option("subscribe", inputTopic)
 .option("kafka.fetch.max.wait.ms", "50")
 .option("kafka.max.partition.fetch.bytes", "10485760") // 10MB
 .load()
 .withColumn("value", substring(col("value"), 0, 500 * 1000))


val withMaybeStatefulTransform =
 if (!isStateful) {
   kafkaStream
 } else {
   kafkaStream
     .groupBy(col("timestamp"), col("value"))
     .count()
     .withColumn("value", concat(col("count"), lit(","), col("value")))
 }


val finalStream = withMaybeStatefulTransform
 .withColumn("value", base64(col("value")))
 // Propagate Kafka ingestion timestamp as a header for end-to-end latency measurement
 .withColumn(
   "headers",
   array(
     struct(
       lit("source-timestamp") as "key",
       unix_millis(col("timestamp")).cast("STRING").cast("BINARY") as "value")))


val currentTimestampUDF = udf(() => System.currentTimeMillis())


val q = finalStream.writeStream
 .trigger(Trigger.ProcessingTime(0))
 .format("kafka")
 .option("kafka.bootstrap.servers", kafkaBootstrapServers)
 .option("topic", outputTopic)
 .option("checkpointLocation", checkpointLocation)
 .option("asyncProgressTrackingEnabled", true)
 .option("kafka.buffer.memory", "67108864")  // 64MB
 .option("kafka.compression.type", "snappy")
 .outputMode("update")
 .queryName("benchRTM")
 .start()


