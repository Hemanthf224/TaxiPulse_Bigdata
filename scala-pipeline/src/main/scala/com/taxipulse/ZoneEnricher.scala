package com.taxipulse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * ZoneEnricher.scala
 * Performs a broadcast join (broadcast()) with the 265-row NYC Taxi Zone lookup table.
 * Avoids a full shuffle stage across millions of records.
 */
object ZoneEnricher {

  def enrich(tripsDf: DataFrame, zoneLookupDf: DataFrame): DataFrame = {
    // Clean and select lookup columns
    val puLookup = zoneLookupDf.select(
      col("LocationID").cast("long").as("pu_loc_id"),
      col("Borough").as("pu_borough"),
      col("Zone").as("pu_zone"),
      col("service_zone").as("pu_service_zone")
    )

    val doLookup = zoneLookupDf.select(
      col("LocationID").cast("long").as("do_loc_id"),
      col("Borough").as("do_borough"),
      col("Zone").as("do_zone"),
      col("service_zone").as("do_service_zone")
    )

    // Broadcast joins for pickup and dropoff locations
    val enriched = tripsDf
      .join(broadcast(puLookup), col("pu_location_id") === col("pu_loc_id"), "left")
      .drop("pu_loc_id")
      .join(broadcast(doLookup), col("do_location_id") === col("do_loc_id"), "left")
      .drop("do_loc_id")

    enriched
  }
}
