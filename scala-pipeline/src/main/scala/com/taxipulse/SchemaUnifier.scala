package com.taxipulse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * SchemaUnifier.scala
 * Normalizes multi-year schema drift across NYC TLC Yellow Taxi, Green Taxi, and FHVHV trip datasets.
 * Uses coalesce for case-insensitive attribute resolution to prevent null key filtering.
 */
object SchemaUnifier {

  val canonicalSchema = StructType(Seq(
    StructField("pickup_ts", TimestampType, true),
    StructField("dropoff_ts", TimestampType, true),
    StructField("pu_location_id", LongType, true),
    StructField("do_location_id", LongType, true),
    StructField("trip_distance_mi", DoubleType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("passenger_count", DoubleType, true),
    StructField("service_type", StringType, true)
  ))

  def unify(df: DataFrame, serviceType: String): DataFrame = {
    if (df == null || df.isEmpty) {
      return df.sparkSession.createDataFrame(df.sparkSession.sparkContext.emptyRDD[org.apache.spark.sql.Row], canonicalSchema)
    }

    val cols = df.columns.map(_.toLowerCase)

    def getCol(possibleNames: String*) = {
      val foundName = df.columns.find(c => possibleNames.exists(_.equalsIgnoreCase(c)))
      foundName match {
        case Some(name) => col(name)
        case None => lit(null)
      }
    }

    serviceType.toLowerCase match {
      case "yellow" =>
        df.select(
          getCol("tpep_pickup_datetime").cast(TimestampType).as("pickup_ts"),
          getCol("tpep_dropoff_datetime").cast(TimestampType).as("dropoff_ts"),
          getCol("PULocationID", "pulocationid").cast(LongType).as("pu_location_id"),
          getCol("DOLocationID", "dolocationid").cast(LongType).as("do_location_id"),
          getCol("trip_distance").cast(DoubleType).as("trip_distance_mi"),
          getCol("fare_amount").cast(DoubleType).as("fare_amount"),
          getCol("tip_amount").cast(DoubleType).as("tip_amount"),
          getCol("total_amount").cast(DoubleType).as("total_amount"),
          getCol("passenger_count").cast(DoubleType).as("passenger_count"),
          lit("yellow").as("service_type")
        )

      case "green" =>
        df.select(
          getCol("lpep_pickup_datetime").cast(TimestampType).as("pickup_ts"),
          getCol("lpep_dropoff_datetime").cast(TimestampType).as("dropoff_ts"),
          getCol("PULocationID", "pulocationid").cast(LongType).as("pu_location_id"),
          getCol("DOLocationID", "dolocationid").cast(LongType).as("do_location_id"),
          getCol("trip_distance").cast(DoubleType).as("trip_distance_mi"),
          getCol("fare_amount").cast(DoubleType).as("fare_amount"),
          getCol("tip_amount").cast(DoubleType).as("tip_amount"),
          getCol("total_amount").cast(DoubleType).as("total_amount"),
          getCol("passenger_count").cast(DoubleType).as("passenger_count"),
          lit("green").as("service_type")
        )

      case "fhvhv" =>
        df.select(
          getCol("pickup_datetime").cast(TimestampType).as("pickup_ts"),
          getCol("dropoff_datetime").cast(TimestampType).as("dropoff_ts"),
          getCol("PULocationID", "pulocationid").cast(LongType).as("pu_location_id"),
          getCol("DOLocationID", "dolocationid").cast(LongType).as("do_location_id"),
          getCol("trip_miles").cast(DoubleType).as("trip_distance_mi"),
          getCol("base_passenger_fare").cast(DoubleType).as("fare_amount"),
          getCol("tips").cast(DoubleType).as("tip_amount"),
          (getCol("base_passenger_fare") + getCol("tolls") + getCol("sales_tax") + getCol("congestion_surcharge") + getCol("tips")).cast(DoubleType).as("total_amount"),
          lit(null).cast(DoubleType).as("passenger_count"),
          lit("fhvhv").as("service_type")
        )

      case _ =>
        throw new IllegalArgumentException(s"Unknown service type: $serviceType")
    }
  }
}
