package com.taxipulse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * WeatherJoiner.scala
 * Parses Open-Meteo semi-structured weather JSON and joins hourly weather metrics
 * onto trip records based on pickup date and hour.
 * 
 * Variety requirement: Integrates semi-structured JSON with structured Parquet trips.
 */
object WeatherJoiner {

  def joinWeather(tripsDf: DataFrame, weatherJsonDf: DataFrame): DataFrame = {
    import weatherJsonDf.sparkSession.implicits._

    // Extract nested hourly array from Open-Meteo JSON schema
    val hourlyData = weatherJsonDf
      .select(explode(arrays_zip(
        col("hourly.time"),
        col("hourly.temperature_2m"),
        col("hourly.precipitation"),
        col("hourly.rain"),
        col("hourly.wind_speed_10m"),
        col("hourly.relative_humidity_2m")
      )).as("w"))
      .select(
        col("w.time").cast(TimestampType).as("weather_ts"),
        col("w.temperature_2m").cast(DoubleType).as("temperature_c"),
        col("w.precipitation").cast(DoubleType).as("precipitation_mm"),
        col("w.rain").cast(DoubleType).as("rain_mm"),
        col("w.wind_speed_10m").cast(DoubleType).as("wind_speed_kmh"),
        col("w.relative_humidity_2m").cast(DoubleType).as("humidity_pct")
      )
      .withColumn("w_date", to_date(col("weather_ts")))
      .withColumn("w_hour", hour(col("weather_ts")))
      .withColumn("is_rainy", when(col("rain_mm") > 0.1, 1).otherwise(0))

    // Prepare trip DataFrame with matching join keys
    val tripsWithKeys = tripsDf
      .withColumn("t_date", to_date(col("pickup_ts")))
      .withColumn("t_hour", hour(col("pickup_ts")))

    // Left join weather metrics onto trip records
    val joined = tripsWithKeys
      .join(
        broadcast(hourlyData),
        col("t_date") === col("w_date") && col("t_hour") === col("w_hour"),
        "left"
      )
      .drop("w_date", "w_hour", "t_date", "t_hour")

    joined
  }
}
