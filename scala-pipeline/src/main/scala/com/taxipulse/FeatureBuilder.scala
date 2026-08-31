package com.taxipulse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * FeatureBuilder.scala
 * Computes analytical features and derived attributes for downstream machine learning
 * and spatial-temporal demand aggregations.
 */
object FeatureBuilder {

  def buildFeatures(df: DataFrame): DataFrame = {
    val durationSec = (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts")))
    val durationMin = (durationSec / 60.0)
    val speedMph = (col("trip_distance_mi") / (durationMin / 60.0))

    val featured = df
      .withColumn("trip_duration_min", round(durationMin, 2))
      .withColumn("avg_speed_mph", round(speedMph, 2))
      .withColumn("pickup_hour", hour(col("pickup_ts")))
      .withColumn("pickup_dayofweek", dayofweek(col("pickup_ts")))
      .withColumn("pickup_month", month(col("pickup_ts")))
      .withColumn("pickup_year", year(col("pickup_ts")))
      .withColumn("is_weekend", when(dayofweek(col("pickup_ts")).isin(1, 7), 1).otherwise(0))
      .withColumn("is_night", when(hour(col("pickup_ts")) >= 22 || hour(col("pickup_ts")) < 5, 1).otherwise(0))
      .withColumn("is_airport_trip", when(
        col("pu_location_id").isin(1, 132, 138) || col("do_location_id").isin(1, 132, 138), 1
      ).otherwise(0))
      .withColumn("fare_per_mile", round(col("fare_amount") / col("trip_distance_mi"), 2))

    featured
  }
}
