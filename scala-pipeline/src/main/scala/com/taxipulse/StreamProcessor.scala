package com.taxipulse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._ // using standard spark logging or just println

object StreamProcessor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TaxiPulse-StreamProcessor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers = "kafka:29092"
    val topic = "taxi-trip-events"

    println(s"Starting Spark Structured Streaming connected to Kafka at $kafkaBrokers, topic: $topic")

    // Define JSON schema for incoming trip events (simplified for the stream)
    val tripSchema = StructType(Array(
      StructField("VendorID", LongType),
      StructField("tpep_pickup_datetime", StringType),
      StructField("tpep_dropoff_datetime", StringType),
      StructField("passenger_count", DoubleType),
      StructField("trip_distance", DoubleType),
      StructField("PULocationID", LongType),
      StructField("DOLocationID", LongType),
      StructField("fare_amount", DoubleType)
    ))

    // Read stream from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    // Parse JSON
    val parsedStream = kafkaStream.select(
      from_json(col("value").cast("string"), tripSchema).alias("data")
    ).select("data.*")

    // Convert string datetime to actual timestamp
    val tripsWithTime = parsedStream
      .withColumn("pickup_time", to_timestamp(col("tpep_pickup_datetime")))
      .withColumn("dropoff_time", to_timestamp(col("tpep_dropoff_datetime")))

    // Load static Taxi Zone lookup table to enrich the stream
    val zonePath = "hdfs://namenode:9000/taxipulse/reference/taxi_zone_lookup.csv"
    
    // We will just try to read it locally or from HDFS if available. 
    // For local testing without HDFS, we will do a conditional load.
    val isLocal = spark.conf.get("spark.master", "").startsWith("local")
    val actualZonePath = if (isLocal) "C:/TaxiPulse_Bigdata/data/reference/taxi_zone_lookup.csv" else zonePath

    val zonesDf = try {
      spark.read.option("header", "true").csv(actualZonePath)
    } catch {
      case _: Exception => spark.emptyDataFrame
    }

    // Real-time Aggregation: 5-minute sliding window (sliding every 1 minute)
    // Counting trips and averaging fare amount per Pickup Location
    val demandAgg = tripsWithTime
      .withWatermark("pickup_time", "10 minutes")
      .groupBy(
        window(col("pickup_time"), "5 minutes", "1 minute"),
        col("PULocationID")
      )
      .agg(
        count("*").alias("trip_count"),
        avg("fare_amount").alias("avg_fare")
      )

    // Enrich with zone names if zones were loaded
    val enrichedDemand = if (!zonesDf.isEmpty) {
      demandAgg.join(zonesDf, demandAgg("PULocationID") === zonesDf("LocationID"), "left")
        .select(
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("Borough"),
          col("Zone"),
          col("trip_count"),
          col("avg_fare")
        )
    } else {
      demandAgg.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("PULocationID"),
        col("trip_count"),
        col("avg_fare")
      )
    }

    // Output to Console
    val query = enrichedDemand.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
