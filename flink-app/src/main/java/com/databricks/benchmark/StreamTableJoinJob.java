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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Objects;
import java.util.Random;
import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

// Delta Lake imports
import org.apache.flink.table.data.RowData;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.connector.file.src.FileSource;

/**
 * Query C: Stream-static join benchmark.
 * Reads from Kafka, assigns a random join key, joins against a static Parquet lookup table
 * using Flink broadcast state, base64-encodes the payload, and writes back to Kafka.
 */
public class StreamTableJoinJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamTableJoinJob.class);
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    // Keys for config properties:
    private static final String KAFKA_BROKERS = "brokers";
    private static final String KAFKA_CONSUME_TOPIC = "consumeTopic";
    private static final String KAFKA_PRODUCE_TOPIC = "produceTopic";
    private static final String JOB_PARALLELISM = "parallelism";
    private static final String JOB_START_FROM_EARLIEST = "startFromEarliest";
    private static final String JOB_CHECKPOINT_INTERVAL = "checkpointInterval";
    private static final String DELTA_TABLE_PATH = "deltaTablePath";
    private static final String LOOKUP_CARDINALITY = "lookupCardinality";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    /**
     * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties
     * or from a local resource when running locally
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env)
            throws IOException {
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    Objects.requireNonNull(StreamTableJoinJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE)).getPath());
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
        final boolean startFromEarliest = Boolean.valueOf(applicationProperties.get("JOB").getProperty(JOB_START_FROM_EARLIEST));

        // Path to Parquet lookup table (e.g. on S3) and its key cardinality
        final String deltaTablePath = applicationProperties.get("JOB").getProperty(DELTA_TABLE_PATH);
        final int lookupCardinality = Integer.valueOf(applicationProperties.get("JOB").getProperty(LOOKUP_CARDINALITY, "10000"));

        final Map<String, String> commonKafkaOptions = new HashMap<>();
        final String clientId = applicationProperties.get("KAFKA").getProperty("clientId", "test.flink1");
        commonKafkaOptions.put("client.id", clientId);

        final Properties consumeKafkaOptions = new Properties();
        consumeKafkaOptions.putAll(commonKafkaOptions);
        consumeKafkaOptions.put("client.dns.lookup", "resolve_canonical_bootstrap_servers_only");

        final Properties produceKafkaOptions = new Properties();
        produceKafkaOptions.putAll(commonKafkaOptions);
        produceKafkaOptions.put("acks", "all");
        produceKafkaOptions.put("retries", String.valueOf(Integer.MAX_VALUE));
        produceKafkaOptions.put("max.in.flight.requests.per.connection", String.valueOf(1));
        produceKafkaOptions.put("compression.type", "snappy");

        if (isLocal(env)) {
            final int parallelism = Integer.valueOf(applicationProperties.get("JOB").getProperty(JOB_PARALLELISM));
            final long checkpointInterval = Long.valueOf(applicationProperties.get("JOB").getProperty(JOB_CHECKPOINT_INTERVAL));
            LOG.info("Running in local mode with parallelism {} and checkpoint interval {}", parallelism, checkpointInterval);
            env.setParallelism(parallelism);
            env.enableCheckpointing(checkpointInterval);
            final CheckpointConfig chkCfg = env.getCheckpointConfig();
            chkCfg.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            chkCfg.setMinPauseBetweenCheckpoints(checkpointInterval);
            chkCfg.setMaxConcurrentCheckpoints(1);
        }

        // Create Kafka source for stream data
        KafkaSource<TimeAndPayload> kafkaSource = KafkaSource.<TimeAndPayload>builder()
            .setBootstrapServers(brokers)
            .setTopics(consumeTopic)
            .setGroupId(clientId)
            .setStartingOffsets(startFromEarliest ? OffsetsInitializer.earliest() : OffsetsInitializer.latest())
            .setProperties(consumeKafkaOptions)
            .setDeserializer(new TimeAndPayloadDeserializer())
            .build();

        // Configure Hadoop S3A filesystem for reading the Parquet lookup table from S3
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com");

        final LogicalType[] fieldTypes = new LogicalType[] { new BigIntType(), new BigIntType() };
        final RowType rowType = RowType.of(fieldTypes, new String[] {"lookupKey", "lookupValue"});

        final ParquetColumnarRowInputFormat format = new ParquetColumnarRowInputFormat(
            hadoopConf,
            rowType,
            InternalTypeInfo.of(rowType),
            500,
            false,
            true
        );

        FileSource<RowData> fileSource = FileSource.forBulkFileFormat(
            format,
            new Path(deltaTablePath)
        ).build();

        // Create Kafka sink
        KafkaSink<JoinedRecord> kafkaSink = KafkaSink.<JoinedRecord>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(new JoinedRecordSerializer(produceTopic))
            .setKafkaProducerConfig(produceKafkaOptions)
            .build();

        // Create broadcast state descriptor for lookup table
        MapStateDescriptor<Long, Long> lookupStateDescriptor =
            new MapStateDescriptor<>("lookup-state", Long.class, Long.class);

        // Read Delta table and create broadcast stream
        // Read with parallelism 1 since the entire table must be broadcast to all operators
        DataStream<RowData> lookupTable = env
            .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source")
            .setParallelism(1);

        BroadcastStream<RowData> broadcastLookupTable = lookupTable
            .broadcast(lookupStateDescriptor);

        // Read Kafka stream and add random join key
        DataStream<TimeAndPayloadWithKey> streamWithKey = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(x -> new TimeAndPayloadWithKey(x.time, truncateArray(x.payload, 500 * 1000),
                (long) new Random().nextInt(lookupCardinality)))
            .name("Add Random Join Key");

        // Apply broadcast join
        BroadcastConnectedStream<TimeAndPayloadWithKey, RowData> connectedStream =
            streamWithKey.connect(broadcastLookupTable);

        DataStream<JoinedRecord> joinedStream = connectedStream
            .process(new BroadcastJoinFunction())
            .name("Broadcast Join");

        // Process joined results
        DataStream<JoinedRecord> processedStream = joinedStream
            .map(x -> new JoinedRecord(x.time, base64Encode(x.payload), x.lookupValue))
            .name("Process Results");

        // Write to Kafka
        processedStream.sinkTo(kafkaSink);

        env.execute("flink stream-table join benchmark");
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
 * Joins stream records against a broadcast lookup table held in Flink's broadcast state.
 * Lookup table rows are ingested via processBroadcastElement; stream records are matched
 * by joinKey in processElement. Records with no matching key are dropped.
 */
class BroadcastJoinFunction extends BroadcastProcessFunction<TimeAndPayloadWithKey, RowData, JoinedRecord> {
    @Override
    public void processElement(TimeAndPayloadWithKey value, ReadOnlyContext ctx, Collector<JoinedRecord> out) throws Exception {
        ReadOnlyBroadcastState<Long, Long> broadcastState = ctx.getBroadcastState(
            new MapStateDescriptor<>("lookup-state", Long.class, Long.class));
        Long lookupValue = broadcastState.get(value.joinKey);
        if (lookupValue != null) {
            out.collect(new JoinedRecord(value.time, value.payload, lookupValue));
        }
    }

    @Override
    public void processBroadcastElement(RowData value, Context ctx, Collector<JoinedRecord> out) throws Exception {
        BroadcastState<Long, Long> broadcastState = ctx.getBroadcastState(
            new MapStateDescriptor<>("lookup-state", Long.class, Long.class));
        long lookupKey = value.getLong(0);
        long lookupValue = value.getLong(1);
        broadcastState.put(lookupKey, lookupValue);
    }
}

/** Serializes JoinedRecord to Kafka, propagating source-timestamp as a header for latency measurement. */
class JoinedRecordSerializer implements KafkaRecordSerializationSchema<JoinedRecord> {
    private String topic;

    public JoinedRecordSerializer(final String topic) {
        this.topic = topic;
    }

    public ProducerRecord<byte[], byte[]> serialize(JoinedRecord element, KafkaSinkContext context, Long flinkTimestamp) {
        final byte[] value = element.payload;
        final byte[] key = null;
        final Integer partition = null;
        final Long kafkaSinkTimestamp = null;
        final Header sourceTimeHeader = new RecordHeader(
            "source-timestamp",
            ByteBuffer.allocate(8).putLong(element.time).array()
        );
        final Collection<Header> headers = Collections.singleton(sourceTimeHeader);
        return new ProducerRecord<>(
            topic,
            partition,
            kafkaSinkTimestamp,
            key,
            value,
            headers
        );
    }
}

/** Deserializes Kafka records into TimeAndPayload using the Kafka record timestamp. */
class TimeAndPayloadDeserializer implements KafkaRecordDeserializationSchema<TimeAndPayload> {
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TimeAndPayload> out) {
    out.collect(new TimeAndPayload(record.timestamp(), record.value()));
  }

  public TypeInformation<TimeAndPayload> getProducedType() {
    return TypeInformation.of(new TypeHint<TimeAndPayload>(){});
  }
}

/** Serializes TimeAndPayload to Kafka, propagating source-timestamp as a header for latency measurement. */
class TimeAndPayloadSerializer implements KafkaRecordSerializationSchema<TimeAndPayload> {
  private String topic;

  public TimeAndPayloadSerializer(final String topic) {
    this.topic = topic;
  }

  public ProducerRecord<byte[], byte[]> serialize(TimeAndPayload element, KafkaSinkContext context, Long flinkTimestamp) {
      final byte[] value = element.payload;
      final byte[] key = null;
      final Integer partition = null;
      final Long kafkaSinkTimestamp = null;
      final Header sourceTimeHeader = new RecordHeader(
        "source-timestamp",
        ByteBuffer.allocate(8).putLong(element.time).array()
      );
      final Collection<Header> headers = Collections.singleton(sourceTimeHeader);

      return new ProducerRecord<>(
        topic,
        partition,
        kafkaSinkTimestamp,
        key,
        value,
        headers
      );
  }
}
