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

package com.databricks.benchmark;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query A (stateless) / Query B (stateful): Kafka-to-Kafka passthrough benchmark. Reads records
 * from Kafka, truncates payload to 500KB, optionally applies a keyed GroupBy+Count via
 * StatefulProcessFunction, base64-encodes, and writes back to Kafka. Toggle isStateful in the JOB
 * config to switch between queries.
 */
public class StreamingJob {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
  private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE =
      "flink-application-properties-dev.json";

  // Keys for config properties:
  private static final String KAFKA_BROKERS = "brokers";
  private static final String KAFKA_CONSUME_TOPIC = "consumeTopic";
  private static final String KAFKA_PRODUCE_TOPIC = "produceTopic";
  private static final String JOB_PARALLELISM = "parallelism";
  private static final String JOB_IS_STATEFUL = "isStateful";
  private static final String JOB_START_FROM_EARLIEST = "startFromEarliest";
  private static final String JOB_CHECKPOINT_INTERVAL = "checkpointInterval";

  private static boolean isLocal(StreamExecutionEnvironment env) {
    return env instanceof LocalStreamEnvironment;
  }

  /**
   * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties or
   * from a local resource when running locally
   */
  private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env)
      throws IOException {
    if (isLocal(env)) {
      LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
      return KinesisAnalyticsRuntime.getApplicationProperties(
          Objects.requireNonNull(
                  StreamingJob.class
                      .getClassLoader()
                      .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE))
              .getPath());
    } else {
      LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
      return KinesisAnalyticsRuntime.getApplicationProperties();
    }
  }

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, Properties> applicationProperties = loadApplicationProperties(env);
    LOG.info("Application properties: {}", applicationProperties);

    final String brokers = applicationProperties.get("KAFKA").getProperty(KAFKA_BROKERS);
    final String consumeTopic = applicationProperties.get("KAFKA").getProperty(KAFKA_CONSUME_TOPIC);
    final String produceTopic = applicationProperties.get("KAFKA").getProperty(KAFKA_PRODUCE_TOPIC);
    final boolean isStateful =
        Boolean.valueOf(applicationProperties.get("JOB").getProperty(JOB_IS_STATEFUL));
    final boolean startFromEarliest =
        Boolean.valueOf(applicationProperties.get("JOB").getProperty(JOB_START_FROM_EARLIEST));

    final Map<String, String> commonKafkaOptions = new HashMap<>();
    final String clientId =
        applicationProperties.get("KAFKA").getProperty("clientId", "test.flink1");
    commonKafkaOptions.put("client.id", clientId);

    final Properties consumeKafkaOptions = new Properties();
    consumeKafkaOptions.putAll(commonKafkaOptions);
    // Required for MSK to resolve broker addresses via bootstrap servers
    consumeKafkaOptions.put("client.dns.lookup", "resolve_canonical_bootstrap_servers_only");

    final Properties produceKafkaOptions = new Properties();
    produceKafkaOptions.putAll(commonKafkaOptions);
    // Ensure exactly-once delivery: wait for all replicas, retry indefinitely, no reordering
    produceKafkaOptions.put("acks", "all");
    produceKafkaOptions.put("retries", String.valueOf(Integer.MAX_VALUE));
    produceKafkaOptions.put("max.in.flight.requests.per.connection", String.valueOf(1));
    produceKafkaOptions.put("compression.type", "snappy");

    if (isLocal(env)) {
      final int parallelism =
          Integer.valueOf(applicationProperties.get("JOB").getProperty(JOB_PARALLELISM));
      final long checkpointInterval =
          Long.valueOf(applicationProperties.get("JOB").getProperty(JOB_CHECKPOINT_INTERVAL));
      LOG.info(
          "Running in local mode with parallelism {} and checkpoint interval {}",
          parallelism,
          checkpointInterval);
      env.setParallelism(parallelism);
      env.enableCheckpointing(checkpointInterval);
      final CheckpointConfig chkCfg = env.getCheckpointConfig();
      chkCfg.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      chkCfg.setMinPauseBetweenCheckpoints(checkpointInterval);
      chkCfg.setMaxConcurrentCheckpoints(1);
    }

    KafkaSource<TimeAndPayload> kafkaSource =
        KafkaSource.<TimeAndPayload>builder()
            .setBootstrapServers(brokers)
            .setTopics(consumeTopic)
            .setGroupId(clientId)
            .setStartingOffsets(
                startFromEarliest ? OffsetsInitializer.earliest() : OffsetsInitializer.latest())
            .setProperties(consumeKafkaOptions)
            .setDeserializer(new TimeAndPayloadDeserializer())
            .build();

    KafkaSink<TimeAndPayload> kafkaSink =
        KafkaSink.<TimeAndPayload>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(new TimeAndPayloadSerializer(produceTopic))
            .setKafkaProducerConfig(produceKafkaOptions)
            .build();

    DataStream<TimeAndPayload> elems =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(x -> new TimeAndPayload(x.time, truncateArray(x.payload, 500 * 1000)));

    DataStream<TimeAndPayload> withMaybeStatefulTransform =
        !isStateful ? elems : (elems.keyBy(x -> x).process(new StatefulProcessFunction()));

    withMaybeStatefulTransform
        .map(x -> new TimeAndPayload(x.time, base64Encode(x.payload)))
        .sinkTo(kafkaSink);
    ;

    env.execute("flink benchmark");
  }

  static byte[] base64Encode(byte[] arr) {
    return Base64.getEncoder().encode(arr);
  }

  static byte[] truncateArray(byte[] arr, int maxLen) {
    if (arr.length > maxLen) {
      return Arrays.copyOfRange(arr, 0, maxLen);
    } else {
      return arr;
    }
  }
}

/**
 * Maintains a per-key count in Flink state and prepends it to the payload. Used for Query B to
 * simulate a stateful GroupBy+Count aggregation.
 */
class StatefulProcessFunction
    extends KeyedProcessFunction<TimeAndPayload, TimeAndPayload, TimeAndPayload> {
  private ValueState<Integer> countState;

  @Override
  public void open(Configuration parameters) throws Exception {
    countState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Integer.class));
  }

  @Override
  public void processElement(TimeAndPayload value, Context ctx, Collector<TimeAndPayload> out)
      throws Exception {
    Integer prevCount = countState.value();
    Integer curCount = prevCount == null ? 1 : (prevCount + 1);
    countState.update(curCount);
    StringBuilder sb = new StringBuilder();
    sb.append(curCount);
    sb.append(',');
    sb.append(new String(value.payload));
    byte[] newPayload = sb.toString().getBytes();
    out.collect(new TimeAndPayload(value.time, newPayload));
  }
}
