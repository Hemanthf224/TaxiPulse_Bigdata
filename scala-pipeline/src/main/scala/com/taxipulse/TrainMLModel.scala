package com.taxipulse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

/**
 * TrainMLModel.scala
 * TaxiPulse — Distributed Machine Learning Pipeline for Trip Fare & ETA Prediction.
 * Upgraded with Advanced Feature Engineering, Target Encoding, and deep GBTs.
 */
object TrainMLModel {

  case class MLConfig(
    inputPath: String = "hdfs://namenode:9000/taxipulse/processed/trips",
    outputPath: String = "hdfs://namenode:9000/taxipulse/models"
  )

  def parseArgs(args: Array[String]): MLConfig = {
    var config = MLConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input" =>
          config = config.copy(inputPath = args(i + 1))
          i += 1
        case "--output" =>
          config = config.copy(outputPath = args(i + 1))
          i += 1
        case _ =>
      }
      i += 1
    }
    config
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    println("=" * 75)
    println("TaxiPulse — Spark MLlib Distributed Predictive Modeling Pipeline (Maxed-Out)")
    println("=" * 75)

    val spark = SparkSession.builder()
      .appName("TaxiPulse-MachineLearning")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .getOrCreate()

    import spark.implicits._

    // 1. Load Preprocessed Dataset
    println(s"[LOAD] Reading processed trip dataset from ${config.inputPath}...")
    var df = spark.read.parquet(config.inputPath)

    // Filter valid numerical bounds for regression targets
    var cleanedDf = df.filter(
      $"trip_distance_mi" > 0.1 && $"trip_distance_mi" < 50.0 &&
      $"fare_amount" >= 2.50 && $"fare_amount" <= 150.0 &&
      $"trip_duration_min" >= 1.0 && $"trip_duration_min" <= 120.0
    )

    // 2. Advanced Feature Engineering
    println("[FEATURE ENGINEERING] Extracting cyclical time and surcharge proxies...")
    cleanedDf = cleanedDf
      .withColumn("sin_hour", sin(lit(2 * math.Pi) * $"pickup_hour" / 24.0))
      .withColumn("cos_hour", cos(lit(2 * math.Pi) * $"pickup_hour" / 24.0))
      .withColumn("log_distance", log1p($"trip_distance_mi"))
      .withColumn("is_weekend", when($"pickup_dayofweek".isin(6, 7), 1.0).otherwise(0.0)) // Assuming 1=Mon, 7=Sun based on spark dayofweek
      .withColumn("is_rush_hour", when($"pickup_dayofweek" < 6 && (($"pickup_hour" >= 7 && $"pickup_hour" <= 9) || ($"pickup_hour" >= 16 && $"pickup_hour" <= 19)), 1.0).otherwise(0.0))
      .withColumn("is_night", when($"pickup_hour" >= 20 || $"pickup_hour" <= 6, 1.0).otherwise(0.0))
      // 132=JFK, 138=LGA, 1=EWR
      .withColumn("is_airport", when($"pu_location_id".isin(132, 138, 1) || $"do_location_id".isin(132, 138, 1), 1.0).otherwise(0.0))

    // 3. Train / Test Split (80% Train, 20% Test) BEFORE Target Encoding to prevent leakage
    val Array(trainDataRaw, testDataRaw) = cleanedDf.randomSplit(Array(0.8, 0.2), seed = 42L)
    
    println("[TARGET ENCODING] Calculating historical means per TLC Zone from Training Data...")
    val globalFareMean = trainDataRaw.select(avg("fare_amount")).first().getDouble(0)
    val globalEtaMean = trainDataRaw.select(avg("trip_duration_min")).first().getDouble(0)

    val puTargetDf = trainDataRaw.groupBy("pu_location_id").agg(
      avg("fare_amount").alias("pu_fare_mean"),
      avg("trip_duration_min").alias("pu_eta_mean")
    )
    
    val doTargetDf = trainDataRaw.groupBy("do_location_id").agg(
      avg("fare_amount").alias("do_fare_mean"),
      avg("trip_duration_min").alias("do_eta_mean")
    )

    // Join encodings and fill missing with global mean
    def applyTargetEncoding(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
      df.join(broadcast(puTargetDf), Seq("pu_location_id"), "left")
        .join(broadcast(doTargetDf), Seq("do_location_id"), "left")
        .na.fill(globalFareMean, Seq("pu_fare_mean", "do_fare_mean"))
        .na.fill(globalEtaMean, Seq("pu_eta_mean", "do_eta_mean"))
    }

    val trainData = applyTargetEncoding(trainDataRaw)
    val testData = applyTargetEncoding(testDataRaw)

    // 4. Vector Assembly
    val featureCols = Array(
      "trip_distance_mi", "log_distance",
      "sin_hour", "cos_hour",
      "is_weekend", "is_rush_hour", "is_night", "is_airport",
      "pu_fare_mean", "do_fare_mean",
      "pu_eta_mean", "do_eta_mean"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val trainAssembled = assembler.transform(trainData).cache()
    val testAssembled = assembler.transform(testData).cache()

    val r2Evaluator = new RegressionEvaluator().setMetricName("r2")
    val rmseEvaluator = new RegressionEvaluator().setMetricName("rmse")
    val maeEvaluator = new RegressionEvaluator().setMetricName("mae")

    println("\n" + "=" * 75)
    println("TRAINING FARE PREDICTION (Gradient Boosted Trees)")
    println("=" * 75)

    r2Evaluator.setLabelCol("fare_amount")
    rmseEvaluator.setLabelCol("fare_amount")
    maeEvaluator.setLabelCol("fare_amount")

    // Deep GBT Regressor for Fare
    val gbtFare = new GBTRegressor()
      .setLabelCol("fare_amount")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setMaxDepth(10)
    
    val gbtFareModel = gbtFare.fit(trainAssembled)
    val gbtFarePredictions = gbtFareModel.transform(testAssembled)

    val gbtFareR2 = r2Evaluator.evaluate(gbtFarePredictions)
    val gbtFareRmse = rmseEvaluator.evaluate(gbtFarePredictions)
    val gbtFareMae = maeEvaluator.evaluate(gbtFarePredictions)
    println(f"  Target Encoded GBT -> R2: $gbtFareR2%.4f | RMSE: $$ $gbtFareRmse%.2f | MAE: $$ $gbtFareMae%.2f")

    println("\n" + "=" * 75)
    println("TRAINING ETA PREDICTION (Gradient Boosted Trees)")
    println("=" * 75)

    r2Evaluator.setLabelCol("trip_duration_min")
    rmseEvaluator.setLabelCol("trip_duration_min")
    maeEvaluator.setLabelCol("trip_duration_min")

    // Deep GBT Regressor for ETA
    val gbtEta = new GBTRegressor()
      .setLabelCol("trip_duration_min")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setMaxDepth(10)
    
    val gbtEtaModel = gbtEta.fit(trainAssembled)
    val gbtEtaPredictions = gbtEtaModel.transform(testAssembled)

    val gbtEtaR2 = r2Evaluator.evaluate(gbtEtaPredictions)
    val gbtEtaRmse = rmseEvaluator.evaluate(gbtEtaPredictions)
    val gbtEtaMae = maeEvaluator.evaluate(gbtEtaPredictions)
    println(f"  Target Encoded GBT -> R2: $gbtEtaR2%.4f | RMSE: $gbtEtaRmse%.2f min | MAE: $gbtEtaMae%.2f min")

    // Save Models
    println("\n[HDFS SAVE] Serializing best-performing GBT models to HDFS...")
    gbtFareModel.write.overwrite().save(s"${config.outputPath}/fare_gbt_model_advanced")
    gbtEtaModel.write.overwrite().save(s"${config.outputPath}/eta_gbt_model_advanced")

    println("\n=== SPARK ML MAXIMIZED EVALUATION SUMMARY ===")
    println(f"Task 1: Fare Prediction (GBT-100) -> R2: $gbtFareR2%.4f | MAE: $$ $gbtFareMae%.2f")
    println(f"Task 2: ETA Prediction  (GBT-100) -> R2: $gbtEtaR2%.4f | MAE: $gbtEtaMae%.2f min")
    
    spark.stop()
  }
}
