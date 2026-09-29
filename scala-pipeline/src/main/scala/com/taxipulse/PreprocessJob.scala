package com.taxipulse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * PreprocessJob.scala
 * Main entry point for TaxiPulse Scala Spark Data Ingestion, Veracity Cleaning,
 * Multi-Source Enrichment, and Feature Engineering Pipeline.
 */
object PreprocessJob {

  def readAndUnifyService(spark: SparkSession, basePath: String, serviceType: String, sample: Boolean, sampleRate: Double): DataFrame = {
    val years = Seq("2022", "2023", "2024")
    val months = (1 to 12).map(m => f"$m%02d")

    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(new java.net.URI(basePath), spark.sparkContext.hadoopConfiguration)

    var unifiedDfs = Seq[DataFrame]()

    for (y <- years; m <- months) {
      val monthPath = new Path(s"$basePath/year=$y/month=$m")
      if (fs.exists(monthPath)) {
        try {
          var monthRaw = spark.read.parquet(monthPath.toString)
          if (sample && !monthRaw.isEmpty) {
            monthRaw = monthRaw.sample(sampleRate)
          }
          if (!monthRaw.isEmpty) {
            val unified = SchemaUnifier.unify(monthRaw, serviceType)
            unifiedDfs = unifiedDfs :+ unified
          }
        } catch {
          case e: Exception =>
            println(s"[INGESTION WARNING] Could not read $serviceType for year $y month $m: ${e.getMessage}")
        }
      }
    }

    if (unifiedDfs.nonEmpty) {
      unifiedDfs.reduce(_.unionByName(_, allowMissingColumns = true))
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], SchemaUnifier.canonicalSchema)
    }
  }

  def main(args: Array[String]): Unit = {
    val config = Config.parse(args)

    println("=" * 70)
    println("TaxiPulse — Scala Spark Big Data Preprocessing Pipeline")
    println(s"Input Path:     ${config.inputPath}")
    println(s"Output Path:    ${config.outputPath}")
    println(s"Reference Path: ${config.referencePath}")
    println(s"Sampling Mode:  ${config.sample} (Rate: ${config.sampleRate})")
    println("=" * 70)

    val builder = SparkSession.builder()
      .appName("TaxiPulse-Preprocessing")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .config("spark.sql.files.ignoreCorruptFiles", "true")

    if (!sys.props.contains("spark.master") && !sys.env.contains("MASTER")) {
      builder.master("local[*]")
    }
    val spark = builder.getOrCreate()

    // 1. Read & Unify Raw Datasets month-by-month to guarantee all 36 months ingestion
    val yellowBasePath = s"${config.inputPath}/yellow"
    val greenBasePath = s"${config.inputPath}/green"
    val fhvhvBasePath = s"${config.inputPath}/fhvhv"
    val zonePath = s"${config.referencePath}/taxi_zone_lookup.csv"
    val weatherPath = s"${config.referencePath}/weather.json"

    println(s"[INGESTION] Reading and unifying raw Parquet trip datasets (Yellow, Green, FHVHV) month-by-month...")

    val yellowUnified = readAndUnifyService(spark, yellowBasePath, "yellow", config.sample, config.sampleRate)
    val greenUnified = readAndUnifyService(spark, greenBasePath, "green", config.sample, config.sampleRate)
    val fhvhvUnified = readAndUnifyService(spark, fhvhvBasePath, "fhvhv", config.sample, config.sampleRate)

    val rawUnified = yellowUnified
      .unionByName(greenUnified, allowMissingColumns = true)
      .unionByName(fhvhvUnified, allowMissingColumns = true)

    val totalRawCount = rawUnified.count()
    println(s"[UNIFIED] Raw unified trip record count across all 36 months: $totalRawCount")

    // 2. Veracity Cleaning across all years (targetYear=0 disables single-month filter)
    println(s"[VERACITY] Executing 13 veracity cleaning rules across all multi-year trip records...")
    val cleaningResult = DataCleaner.clean(rawUnified, targetYear = 0, targetMonth = 0)
    val cleanedDf = cleaningResult.cleanedDf

    // 3. Zone Enrichment (Broadcast Join)
    println(s"[ENRICHMENT] Reading Taxi Zone Lookup from $zonePath...")
    val zoneLookupDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zonePath)

    val zoneEnrichedDf = ZoneEnricher.enrich(cleanedDf, zoneLookupDf)

    // 4. Weather Enrichment (Semi-Structured JSON Join)
    println(s"[WEATHER] Reading Open-Meteo Weather JSON from $weatherPath...")
    val weatherJsonDf = spark.read
      .option("multiline", "true")
      .json(weatherPath)

    val weatherEnrichedDf = WeatherJoiner.joinWeather(zoneEnrichedDf, weatherJsonDf)

    // 5. Feature Engineering
    println(s"[FEATURE BUILDER] Deriving spatial, temporal, and rate features...")
    val finalFeaturedDf = FeatureBuilder.buildFeatures(weatherEnrichedDf)

    // 6. Write Quantified Data Quality Report
    val reportOutputPath = "docs/data_quality_report.md"
    QualityReport.generateReport(cleaningResult.metrics, reportOutputPath)

    // 7. Save Processed Dataset back to HDFS
    val outputTripsPath = s"${config.outputPath}/trips"
    println(s"[HDFS WRITE] Writing processed Parquet dataset to $outputTripsPath...")

    finalFeaturedDf
      .write
      .mode("overwrite")
      .partitionBy("pickup_year", "pickup_month")
      .parquet(outputTripsPath)

    println("=" * 70)
    println("[SUCCESS] Preprocessing job completed successfully!")
    println(s"Processed data written to: $outputTripsPath")
    println("=" * 70)

    spark.stop()
  }
}
