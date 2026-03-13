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

// Cell 1 - Imports
import org.apache.spark.sql.functions.{col, floor, map_from_entries}

// Cell 2 - Configuration
val bootstrapServers = sys.env("KAFKA_BOOTSTRAP_SERVERS")
val sinkTopic = sys.env("OUTPUT_TOPIC")
val filterMillis = 60 * 1000 * 5 // 5 minutes warmup filter
val windowInSec = 0 // set > 0 if using windowed aggregation

// Cell 3 - Read from Kafka and compute latency
val kafkaSinkData = spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrapServers)
  .option("subscribe", sinkTopic)
  .option("includeHeaders", "true")
  .load()
  .withColumn("headers-map", map_from_entries(col("headers")))
  .withColumn("source-timestamp",
    col("`headers-map`").getItem("source-timestamp").cast("STRING").cast("BIGINT"))
  .withColumn("sink-timestamp",
    (col("timestamp").cast("double") * 1000).cast("long"))
  .withColumn("latency",
    if (windowInSec <= 0) {
      col("sink-timestamp") - col("source-timestamp")
    } else {
      col("sink-timestamp") -
        ((floor(col("source-timestamp").divide(windowInSec * 1000)) + 1) * windowInSec * 1000).cast("double")
    }
  )

kafkaSinkData.createOrReplaceTempView("sink_data")

// Cell 4 - Add relative time
spark.sql("""
  CREATE OR REPLACE TEMP VIEW sink_data_with_time AS
  SELECT
    (CAST(`source-timestamp` AS BIGINT) -
     (SELECT MIN(CAST(`source-timestamp` AS BIGINT)) FROM sink_data)) AS time,
    *
  FROM sink_data
""")

// Cell 5 - Record counts
display(spark.sql(s"""
  SELECT
    COUNT(CASE WHEN time > $filterMillis THEN 1 END) AS kept,
    COUNT(CASE WHEN time <= $filterMillis THEN 1 END) AS filtered,
    COUNT(*) AS total
  FROM sink_data_with_time
"""))

// Cell 6 - Latency percentiles (after warmup filter)
display(spark.sql(s"""
  SELECT
    percentile_approx(latency, 0.0, 100000)  AS p0,
    percentile_approx(latency, 0.5, 100000)  AS p50,
    percentile_approx(latency, 0.9, 100000)  AS p90,
    percentile_approx(latency, 0.95, 100000) AS p95,
    percentile_approx(latency, 0.99, 100000) AS p99,
    percentile_approx(latency, 1.0, 100000)  AS p100
  FROM sink_data_with_time
  WHERE time > $filterMillis
"""))
