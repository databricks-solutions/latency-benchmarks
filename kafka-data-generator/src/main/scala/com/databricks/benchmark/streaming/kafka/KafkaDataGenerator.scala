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

package com.databricks.benchmark.streaming.kafka

import java.util.{Properties, Timer, TimerTask}
import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

import org.apache.kafka.clients.producer._

object KafkaDataGenerator extends App {

  // scalastyle:off
  println(s"CLI args passed in ${args.toList}")
  if (args.length == 3) {
    run(args(0), args(1), args(2).toLong)
  } else if (args.length == 4) {
    run(args(0), args(1), args(2).toLong, Some(args(3)))
  } else if (args.length == 5) {
    run(args(0), args(1), args(2).toLong, Some(args(3)), Some(args(4).toInt))
  } else {
    // scalastyle:off
    println("Needs 3 or 4 parameters: broker url, topic name, rate, compression type (optional), record size (optional)")
  }

  def run(
      url: String,
      topicName: String,
      throughput: Long,
      compressionType: Option[String] = None,
      recordSize: Option[Int] = None): Unit = {

    val sampleLogData = """
24/01/15 10:30:01 INFO SparkContext: Running Spark version 4.0.0
24/01/15 10:30:01 INFO SparkContext: OS info Linux, 5.15.0-91-generic, amd64
24/01/15 10:30:01 INFO SparkContext: Java version 17.0.9
24/01/15 10:30:01 INFO ResourceUtils: ==============================================================
24/01/15 10:30:01 INFO ResourceUtils: No custom resources configured for spark.driver.
24/01/15 10:30:01 INFO ResourceUtils: ==============================================================
24/01/15 10:30:01 INFO SparkContext: Submitted application: Spark Streaming Benchmark
24/01/15 10:30:01 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(memory -> name: memory, amount: 4096, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
24/01/15 10:30:01 INFO ResourceProfile: Limiting resource is cpu
24/01/15 10:30:01 INFO ResourceProfileManager: Added ResourceProfile id: 0
24/01/15 10:30:02 INFO SecurityManager: Changing view acls to: sparkuser
24/01/15 10:30:02 INFO SecurityManager: Changing modify acls to: sparkuser
24/01/15 10:30:02 INFO SecurityManager: Changing view acls groups to:
24/01/15 10:30:02 INFO SecurityManager: Changing modify acls groups to:
24/01/15 10:30:02 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: sparkuser; groups with view permissions: EMPTY; users with modify permissions: sparkuser; groups with modify permissions: EMPTY
24/01/15 10:30:02 INFO Utils: Successfully started service 'sparkDriver' on port 41967.
24/01/15 10:30:02 INFO SparkEnv: Registering MapOutputTracker
24/01/15 10:30:02 INFO SparkEnv: Registering BlockManagerMaster
24/01/15 10:30:02 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
24/01/15 10:30:02 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
24/01/15 10:30:02 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
24/01/15 10:30:02 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-a1b2c3d4-e5f6-7890-abcd-ef1234567890
24/01/15 10:30:02 INFO MemoryStore: MemoryStore started with capacity 4.0 GiB
24/01/15 10:30:02 INFO SparkEnv: Registering OutputCommitCoordinator
24/01/15 10:30:03 INFO Utils: Successfully started service 'SparkUI' on port 4040.
24/01/15 10:30:03 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://master:7077...
24/01/15 10:30:03 INFO TransportClientFactory: Successfully created connection to master/10.0.0.1:7077
24/01/15 10:30:03 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20240115103003-0001
24/01/15 10:30:03 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 43567.
24/01/15 10:30:03 INFO NettyBlockTransferService: Server created on 10.0.0.2:43567
24/01/15 10:30:03 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
24/01/15 10:30:03 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.0.0.2, 43567, None)
24/01/15 10:30:03 INFO BlockManagerMasterEndpoint: Registering block manager 10.0.0.2:43567 with 4.0 GiB RAM, BlockManagerId(driver, 10.0.0.2, 43567, None)
24/01/15 10:30:03 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.0.0.2, 43567, None)
24/01/15 10:30:03 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.0.0.2, 43567, None)
24/01/15 10:30:04 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
24/01/15 10:30:04 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir
24/01/15 10:30:04 INFO SharedState: Warehouse path is 'file:/opt/spark/spark-warehouse'.
24/01/15 10:30:05 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
24/01/15 10:30:05 INFO KafkaSourceProvider: Setting kafka consumer configuration: bootstrap.servers = broker1:9092,broker2:9092,broker3:9092
24/01/15 10:30:05 INFO KafkaSourceProvider: Setting kafka consumer configuration: subscribe = benchmark-input
24/01/15 10:30:05 INFO KafkaSourceProvider: Setting kafka consumer configuration: startingOffsets = latest
24/01/15 10:30:06 INFO ConsumerConfig: ConsumerConfig values:
	auto.commit.interval.ms = 5000
	auto.offset.reset = earliest
	bootstrap.servers = [broker1:9092, broker2:9092, broker3:9092]
	check.crcs = true
	client.dns.lookup = use_all_dns_ips
	client.id = consumer-spark-kafka-source-1
	fetch.max.bytes = 52428800
	fetch.max.wait.ms = 500
	fetch.min.bytes = 1
	group.id = spark-kafka-source-benchmark
	heartbeat.interval.ms = 3000
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	max.partition.fetch.bytes = 1048576
	max.poll.interval.ms = 300000
	max.poll.records = 500
	session.timeout.ms = 45000
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
24/01/15 10:30:06 INFO AppInfoParser: Kafka version: 3.6.1
24/01/15 10:30:06 INFO AppInfoParser: Kafka commitId: abcdef1234567890
24/01/15 10:30:06 INFO KafkaConsumer: [Consumer clientId=consumer-spark-kafka-source-1, groupId=spark-kafka-source-benchmark] Subscribed to topic(s): benchmark-input
24/01/15 10:30:06 INFO MicroBatchExecution: Starting new streaming query
24/01/15 10:30:06 INFO MicroBatchExecution: Stream started from {}
"""
    // scalastyle:off
    println(s"Producing to $url topic $topicName at $throughput records / sec")

    // create instance for properties to access producer configs
    val props: Properties = new Properties()

    // Assign localhost id
    props.put("bootstrap.servers", url)

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    compressionType.foreach { c =>
      props.put("compression.type", c)
    }

    val producer: Producer[String, String] = new KafkaProducer[String, String](props)

    val success = new AtomicLong(0)

    new Timer().scheduleAtFixedRate(new TimerTask() {
      override def run(): Unit = {
        // scalastyle:off
        println("Throughput: " + success.getAndSet(0) + " requests/sec")
      }
    }, 1000, 1000)

    var i = 0L;
    val startTime = System.nanoTime
    val delay = (Math.pow(10, 9) / throughput).asInstanceOf[Long]
    var nextDeadline = startTime + delay
    val rand = new Random()
    while (true) {
      var currentTime = System.nanoTime
      if (currentTime >= nextDeadline) {
        i += 1
        nextDeadline = startTime + (i * delay)
        val payload: String = if (recordSize.isDefined) {
          val start = rand.nextInt(sampleLogData.length)
          var left = recordSize.get
          val sb = new StringBuilder()
          val numRandomBytes = math.min(left, 8)
          sb.append(rand.alphanumeric.take(numRandomBytes).mkString)
          left = left - numRandomBytes
          val end = Math.min(sampleLogData.length, start + recordSize.get)
          sb.append(sampleLogData.substring(start, end))
          val dataLength = sampleLogData.length - (end - start)
          left = left - dataLength
          while (left > 0) {
            if (left > sampleLogData.length) {
              sb.append(sampleLogData)
              left = left - sampleLogData.length
            } else {
              sb.append(sampleLogData.substring(0, left))
              left = 0
            }
          }
          sb.toString()
        } else {
          java.lang.Long.toString(System.currentTimeMillis())
        }

        producer.send(
          new ProducerRecord[String, String](
            topicName,
            java.lang.Long.toString(i),
            payload
          ),
          new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              if (e != null) {
                // scalastyle:off
                println(s"Got exception producing to kafka: $e")
              } else {
                success.incrementAndGet()
              }
            }
          }
        );

        currentTime = System.nanoTime
        val sleepTime =
          if ((nextDeadline - currentTime) > 0) nextDeadline - currentTime
          else 0
        if (sleepTime > 0) {
          val sleepTimeMs = sleepTime / 1000000
          val sleepTimeNano = (sleepTime - sleepTimeMs * 1000000).toInt
          Thread.sleep(sleepTimeMs, sleepTimeNano)
        }
      }
    }
  }
}
