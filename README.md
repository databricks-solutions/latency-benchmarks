# Streaming Latency Benchmark Suite

End-to-end latency benchmarks for comparing Apache Spark Structured Streaming Real-Time Mode and Apache Flink on identical streaming workloads. All benchmarks read from Kafka, apply a transformation, and write back to Kafka. Latency is measured by propagating a `source-timestamp` header through each record.

## Benchmarks

### Query A: Stateless Transformation

Reads each row from Kafka, truncates the payload to 500KB, base64 encodes it, and writes it back to Kafka. No state is maintained.

- **Spark:** `spark/q1.scala` (set `isStateful = false`)
- **Flink:** `StreamingJob` (set `isStateful = false`)

### Query B: Stateful Aggregation

Same as Query A, but additionally performs a GroupBy + Count aggregation (counts the number of occurrences of each input string after truncation).

- **Spark:** `spark/q1.scala` (set `isStateful = true`)
- **Flink:** `StreamingJob` (set `isStateful = true`)

### Query C: Stream-Static Join

Joins the input stream against a static lookup table (10,000 rows mapping `x -> x * 2`). Each input row is assigned a random join key in `[0, 10000)` and joined with the lookup table via a broadcast join.

- **Spark:** `spark/q2.scala` (requires running `utils/static.scala` first to generate the lookup table)
- **Flink:** `StreamTableJoinJob` (reads the lookup table from S3 as Parquet)

## Prerequisites

### Kafka

You need a Kafka cluster with input and output topics created for each benchmark. We used an AWS MSK cluster with 3 brokers, each with 8 cores.

```bash
# Example: create topics
kafka-topics.sh --create --topic input --partitions 40 --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
kafka-topics.sh --create --topic output --partitions 40 --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
```

### Data Generator

The `kafka-data-generator/` directory contains a Scala application that produces synthetic log data to Kafka at a configurable rate.

```bash
# Build
cd kafka-data-generator
sbt assembly    # or: mvn package

# Run — args: <broker-url> <topic> <rate> [compression-type] [record-size]
java -jar target/kafka-data-generator.jar \
  $KAFKA_BOOTSTRAP_SERVERS \
  input \
  10000 \
  snappy \
  1200
```

This produces 10,000 rows/sec of ~1200 byte records (random substrings from a sample Spark driver log).

## Running on Spark (Databricks)

The Spark benchmarks are notebook-style Scala scripts designed to run on a Databricks cluster with Structured Streaming. They read configuration from environment variables.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Comma-separated Kafka broker addresses |
| `INPUT_TOPIC` | Yes | Kafka topic to consume from |
| `OUTPUT_TOPIC` | Yes | Kafka topic to produce to |
| `CHECKPOINT_LOCATION` | Yes | Path for streaming checkpoint (e.g. a Unity Catalog Volume) |
| `STATIC_TABLE_LOCATION` | For Query C | Path where the static lookup table is written/read |
| `LOOKUP_CARDINALITY` | No | Number of rows in the lookup table (default: `10000`) |

### Running the Benchmarks

1. **Start the data generator** (see above) targeting your input topic.

2. **Query A / Query B** — Run `spark/q1.scala` in a Databricks notebook. Toggle `isStateful` at the top of the script.

3. **Query C** — First run `utils/static.scala` to generate the lookup table, then run `spark/q2.scala`.

4. **Analyze results** — After the benchmark completes, run `utils/query_latency.scala` to compute latency percentiles from the output topic.

### Spark Configuration Notes

The scripts set several Spark configs for optimal streaming performance:

- Stateful queries configure RocksDB state store settings and reduce shuffle buffer wait time
- `q2.scala` supports both `RealTimeTrigger` and `Trigger.ProcessingTime(0)` — toggle at the top of the script

> **Note:** Real-Time Mode (RTM) must be enabled on the Databricks cluster. See the [Databricks documentation on Real-Time Mode](https://docs.databricks.com/en/structured-streaming/real-time.html) for setup instructions.

## Running on Flink (AWS Managed Flink 1.18)

The Flink benchmarks have been tested on **AWS Managed Flink** (Amazon Managed Service for Apache Flink). The application is a Java project built with Gradle.

### Build

```bash
cd flink-app
./package.sh
# Output in: build/libs/
#   flink-app-<version>-streaming-job.jar        (Query A/B: StreamingJob)
#   flink-app-<version>-stream-table-join-job.jar (Query C: StreamTableJoinJob)
```

### Configuration

Flink jobs load configuration from **runtime properties** (when deployed to AWS Managed Flink) or from `flink-application-properties-dev.json` (when running locally).

#### Property Groups

**KAFKA:**

| Property | Required | Description |
|---|---|---|
| `brokers` | Yes | Kafka bootstrap servers |
| `consumeTopic` | Yes | Input topic |
| `produceTopic` | Yes | Output topic |
| `clientId` | No | Kafka client ID (default: `test.flink1`) |

**JOB:**

| Property | Required | Description |
|---|---|---|
| `parallelism` | Local only | Number of parallel task slots |
| `isStateful` | Query A/B | Enable stateful aggregation (`true`/`false`) |
| `startFromEarliest` | No | Start consuming from earliest offset |
| `checkpointInterval` | Local only | Checkpoint interval in ms |
| `deltaTablePath` | Query C | S3 path to the Parquet lookup table |
| `lookupCardinality` | No | Number of lookup table rows (default: `10000`) |

### Deploying to AWS Managed Flink

1. Build the fat JARs with `./package.sh`.
2. Upload the appropriate JAR to S3.
3. Create an AWS Managed Flink application pointing to the JAR.
4. Set the runtime properties under the `KAFKA` and `JOB` property groups in the Managed Flink console.
5. Start the application.

### Running Locally

Edit `flink-app/src/main/resources/flink-application-properties-dev.json` with your Kafka and job settings, then:

```bash
cd flink-app
gradle run
```

## Launch Sequence

For a full benchmark run:

1. Create new input and output Kafka topics
2. Configure and start the Flink or Spark application
3. Start the data generator targeting the input topic
4. Run both for 30 minutes
5. Stop the data generator
6. Stop the streaming application after it drains remaining records
7. Run `utils/query_latency.scala` to analyze latency from the output topic

## Analyzing Results

The `utils/query_latency.scala` script reads from the output Kafka topic, extracts the `source-timestamp` header, and computes end-to-end latency percentiles (p0, p50, p90, p95, p99, p100). It filters out a configurable warmup period (default: 5 minutes) to exclude startup transients.

## Benchmark Results

End-to-end latency percentiles (in milliseconds) after filtering out a 5-minute warmup period. All benchmarks used 10,000 rows/sec. Where multiple runs were performed, the worst (max) value across runs is reported for Spark and the best (min) value is reported for Flink.

### Query A: Stateless Transformation

| Engine | p50 | p90 | p95 | p99 |
|--------|-----|-----|-----|-----|
| Spark RTM | 1 | 1 | 2 | 3 |
| Flink | 1 | 1 | 2 | 3 |

#### Query B

| Engine | p50 | p90 | p95 | p99 |
|--------|-----|-----|-----|-----|
| Spark RTM | 7 | 11 | 11 | 14 |
| Flink | 16 | 26 | 32 | 45 |

### Query C: Stream-Static Join

| Engine | p50 | p90 | p95 | p99 |
|--------|-----|-----|-----|-----|
| Spark RTM | 1 | 1 | 2 | 9 |
| Flink | 32 | 73 | 83 | 95 |

## Project Structure

```
latency-bench/
  kafka-data-generator/   # Scala app that produces synthetic data to Kafka
  flink-app/              # Java Flink application (Query A/B + Query C)
  spark/
    q1.scala              # Spark: stateless (Query A) / stateful (Query B)
    q2.scala              # Spark: stream-static join (Query C)
  utils/
    static.scala          # Generates the static lookup table for Query C
    query_latency.scala   # Computes latency percentiles from output topic
```
