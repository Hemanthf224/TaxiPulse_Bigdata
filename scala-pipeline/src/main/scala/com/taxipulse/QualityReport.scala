package com.taxipulse

import java.io.{File, PrintWriter}

/**
 * QualityReport.scala
 * Writes quantified Veracity Data Quality Report to docs/data_quality_report.md
 * providing exact statistics and percentages for viva defense.
 */
object QualityReport {

  def generateReport(metrics: Map[String, Long], outputPath: String): Unit = {
    val initialCount = metrics.getOrElse("00_initial_count", 0L)
    val finalCount = metrics.getOrElse("99_final_clean_count", 0L)
    val totalRemoved = initialCount - finalCount
    val retentionPct = if (initialCount > 0) (finalCount.toDouble / initialCount * 100.0) else 0.0

    val reportText = new StringBuilder()
    reportText.append("# TaxiPulse — Data Quality & Veracity Report\n\n")
    reportText.append("> **Plain Language Summary:** Out of all raw taxi records processed, invalid trips (such as negative fares, zero distances, and taximeter hardware glitches) were systematically filtered to ensure clean, high-fidelity datasets for real-time demand forecasting.\n\n")
    reportText.append("## Overall Data Quality Metrics\n\n")
    reportText.append(f"- **Total Raw Trip Records Ingested:** $initialCount%,d\n")
    reportText.append(f"- **Total Cleaned Trip Records Retained:** $finalCount%,d\n")
    reportText.append(f"- **Total Anomalous Records Filtered:** $totalRemoved%,d\n")
    reportText.append(f"- **Overall Data Retention Rate:** $retentionPct%.2f%%\n\n")

    reportText.append("## Detailed Veracity Filter Breakdown\n\n")
    reportText.append("| Rule # | Filter Description | Real-World Justification | Rows Removed | % of Raw Data |\n")
    reportText.append("|---|---|---|---|---|\n")

    val rules = List(
      ("01_null_keys_removed", "Null Keys", "Missing pickup/dropoff timestamp or zone ID"),
      ("02_invalid_zones_removed", "Invalid Zone IDs", "Location IDs outside valid 1-263 range"),
      ("03_non_positive_fare_removed", "Non-Positive Fare", "Refunded, voided, or test transactions (fare <= 0)"),
      ("04_absurd_fare_removed", "Absurd Fare", "Data entry errors (fare > $1,000)"),
      ("05_non_positive_distance_removed", "Non-Positive Distance", "Trips canceled before vehicle movement (dist <= 0)"),
      ("06_absurd_distance_removed", "Absurd Distance", "Out-of-state GPS glitches (dist > 200 miles)"),
      ("07_time_inversion_removed", "Time Inversion", "Dropoff timestamp earlier than pickup timestamp"),
      ("08_duration_anomalies_removed", "Duration Anomalies", "Accidental starts (<60s) or unclosed meters (>6h)"),
      ("10_impossible_speed_removed", "Impossible Speed", "GPS anomalies (>90 mph or <0.5 mph)"),
      ("11_out_of_period_removed", "Out-of-Period", "Hardware desync stray dates outside file month"),
      ("12_passenger_anomalies_removed", "Passenger Count", "Unrealistic passenger counts (>8 passengers)"),
      ("13_duplicates_removed", "Duplicate Records", "Re-transmitted telemetry packets or gateway double submissions")
    )

    for ((key, name, reason) <- rules) {
      val removed = metrics.getOrElse(key, 0L)
      val pct = if (initialCount > 0) (removed.toDouble / initialCount * 100.0) else 0.0
      reportText.append(f"| ${key.substring(0, 2)} | $name | $reason | $removed%,d | $pct%.2f%% |\n")
    }

    // Write report file locally
    val file = new File(outputPath)
    file.getParentFile.mkdirs()
    val pw = new PrintWriter(file)
    try {
      pw.write(reportText.toString())
    } finally {
      pw.close()
    }

    println(s"[QUALITY REPORT] Report generated successfully at: $outputPath")
  }
}
