package com.taxipulse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * DataCleaner.scala
 * Executes veracity handling and data quality cleaning rules on unified NYC taxi trip records.
 * Tracks row removal metrics for the Data Quality Report.
 */
object DataCleaner {

  case class CleaningResult(
    cleanedDf: DataFrame,
    metrics: Map[String, Long]
  )

  def clean(inputDf: DataFrame, targetYear: Int = 2024, targetMonth: Int = 1): CleaningResult = {
    var df = inputDf
    var metrics = Map[String, Long]()

    val initialCount = df.count()
    metrics += ("00_initial_count" -> initialCount)

    // Rule 1: Null Keys (null pickup/dropoff timestamp or location ID)
    // Real-world reason: unusable record for spatial/temporal modeling
    val nullKeyCondition = col("pickup_ts").isNotNull &&
      col("dropoff_ts").isNotNull &&
      col("pu_location_id").isNotNull &&
      col("do_location_id").isNotNull
    val dfNullsCleaned = df.filter(nullKeyCondition)
    metrics += ("01_null_keys_removed" -> (initialCount - dfNullsCleaned.count()))
    df = dfNullsCleaned

    // Rule 2: Invalid Zone IDs (outside valid NYC Taxi Zone range 1-263)
    // Real-world reason: Zone 264/265 represent Unknown/Outside NYC; IDs outside 1-265 are corrupted records
    val prevCount2 = df.count()
    val validZoneCondition = col("pu_location_id").between(1, 263) && col("do_location_id").between(1, 263)
    val dfZonesCleaned = df.filter(validZoneCondition)
    metrics += ("02_invalid_zones_removed" -> (prevCount2 - dfZonesCleaned.count()))
    df = dfZonesCleaned

    // Rule 3: Non-positive fare (fare_amount <= 0)
    // Real-world reason: fare_amount <= 0 represents voided, refunded, or system test trips, not real commercial rides
    val prevCount3 = df.count()
    val validFareCondition = col("fare_amount") > 0.0
    val dfFareCleaned = df.filter(validFareCondition)
    metrics += ("03_non_positive_fare_removed" -> (prevCount3 - dfFareCleaned.count()))
    df = dfFareCleaned

    // Rule 4: Absurd fare (fare_amount > 1000)
    // Real-world reason: taxi rides over $1,000 in NYC urban limits represent manual data entry errors or corruption
    val prevCount4 = df.count()
    val maxFareCondition = col("fare_amount") <= 1000.0
    val dfAbsurdFareCleaned = df.filter(maxFareCondition)
    metrics += ("04_absurd_fare_removed" -> (prevCount4 - dfAbsurdFareCleaned.count()))
    df = dfAbsurdFareCleaned

    // Rule 5: Non-positive distance (trip_distance_mi <= 0)
    // Real-world reason: trip_distance <= 0 means passenger canceled before vehicle moved or taximeter failed
    val prevCount5 = df.count()
    val validDistCondition = col("trip_distance_mi") > 0.0
    val dfDistCleaned = df.filter(validDistCondition)
    metrics += ("05_non_positive_distance_removed" -> (prevCount5 - dfDistCleaned.count()))
    df = dfDistCleaned

    // Rule 6: Absurd distance (trip_distance_mi > 200)
    // Real-world reason: NYC metro span is ~35 miles; trips > 200 miles represent GPS noise or out-of-state glitches
    val prevCount6 = df.count()
    val maxDistCondition = col("trip_distance_mi") <= 200.0
    val dfAbsurdDistCleaned = df.filter(maxDistCondition)
    metrics += ("06_absurd_distance_removed" -> (prevCount6 - dfAbsurdDistCleaned.count()))
    df = dfAbsurdDistCleaned

    // Rule 7: Time inversion (dropoff_ts <= pickup_ts)
    // Real-world reason: dropoff timestamp occurring before or equal to pickup timestamp indicates clock or meter desync
    val prevCount7 = df.count()
    val timeOrderCondition = col("dropoff_ts") > col("pickup_ts")
    val dfTimeCleaned = df.filter(timeOrderCondition)
    metrics += ("07_time_inversion_removed" -> (prevCount7 - dfTimeCleaned.count()))
    df = dfTimeCleaned

    // Rule 8 & 9: Duration anomalies (< 60 seconds or > 6 hours)
    // Real-world reason: < 60s represents accidental trip starts; > 6h represents drivers forgetting to close meter
    val prevCount8 = df.count()
    val durationSec = (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts")))
    val durationCondition = durationSec.between(60, 21600) // 60 sec to 6 hours
    val dfDurationCleaned = df.filter(durationCondition)
    metrics += ("08_duration_anomalies_removed" -> (prevCount8 - dfDurationCleaned.count()))
    df = dfDurationCleaned

    // Rule 10: Impossible speed (> 90 mph or < 0.5 mph)
    // Real-world reason: avg speed > 90 mph in NYC traffic indicates GPS timestamp error; < 0.5 mph indicates stuck vehicle
    val prevCount10 = df.count()
    val speedMph = (col("trip_distance_mi") / (durationSec / 3600.0))
    val speedCondition = speedMph.between(0.5, 90.0)
    val dfSpeedCleaned = df.filter(speedCondition)
    metrics += ("10_impossible_speed_removed" -> (prevCount10 - dfSpeedCleaned.count()))
    df = dfSpeedCleaned

    // Rule 11: Out-of-period records (pickup date outside expected range or month/year)
    // Real-world reason: TLC datasets famously contain stray dates (e.g. 2002 or 2098) due to corrupted taximeter hardware
    val prevCount11 = df.count()
    val periodCondition = if (targetYear > 0 && targetMonth > 0) {
      year(col("pickup_ts")) === targetYear && month(col("pickup_ts")) === targetMonth
    } else {
      year(col("pickup_ts")).between(2022, 2024)
    }
    val dfPeriodCleaned = df.filter(periodCondition)
    metrics += ("11_out_of_period_removed" -> (prevCount11 - dfPeriodCleaned.count()))
    df = dfPeriodCleaned

    // Rule 12: Passenger count anomaly (> 8 passengers)
    // Real-world reason: passenger count > 8 exceeds standard taxi/SUV capacity limits (FHVHV nulls preserved)
    val prevCount12 = df.count()
    val passengerCondition = col("passenger_count").isNull || col("passenger_count").between(1, 8)
    val dfPassCleaned = df.filter(passengerCondition)
    metrics += ("12_passenger_anomalies_removed" -> (prevCount12 - dfPassCleaned.count()))
    df = dfPassCleaned

    // Rule 13: Exact duplicates
    // Real-world reason: duplicate trip submission records from payment gateways or re-sent telemetry packets
    val prevCount13 = df.count()
    val dfDupsCleaned = df.dropDuplicates()
    metrics += ("13_duplicates_removed" -> (prevCount13 - dfDupsCleaned.count()))
    df = dfDupsCleaned

    val finalCount = df.count()
    metrics += ("99_final_clean_count" -> finalCount)

    CleaningResult(df, metrics)
  }
}
