# TaxiPulse — Big Data Scalable Taxi Demand & Analytics Pipeline

A distributed, high-performance Big Data analytics and preprocessing pipeline built with **Apache Spark (Scala)** running on **Hadoop YARN** and **Hadoop Distributed File System (HDFS)**.

TaxiPulse unifies, cleans, enriches, and processes multi-year NYC Taxi and Ride-hailing datasets (Yellow Taxi, Green Taxi, FHVHV / Uber & Lyft) integrated with hourly meteorological weather data and geographical zone lookups.

---

## Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                                 TaxiPulse Pipeline                                |
+-----------------------------------------------------------------------------------+
|  Storage Layer:         Hadoop HDFS (Replication Factor = 3)                      |
|  Resource Manager:      Hadoop YARN (ResourceManager + 3 NodeManagers)           |
|  Processing Engine:     Apache Spark 3.x (Scala DataFrames & RDDs)                 |
|  Containerization:      Docker Compose Multi-Node Distributed Cluster             |
+-----------------------------------------------------------------------------------+
```

### Components:
* **Hadoop HDFS**: Stores multi-gigabyte raw input trip files and partitioned Snappy Parquet outputs across 3 DataNodes.
* **Hadoop YARN**: Handles dynamic resource allocation, memory management, and distributed executor scheduling.
* **Apache Spark (Scala)**: Performs schema unification, 13 veracity cleaning rules, spatial/temporal feature engineering, and broadcast/temporal joins.
* **Data Sources**:
  * Yellow Taxi Parquet Datasets (2022–2024)
  * Green Taxi Parquet Datasets (2022–2024)
  * FHVHV High-Volume For-Hire Vehicle Datasets (Uber/Lyft)
  * Open-Meteo Weather JSON Dataset
  * NYC Taxi Zone Lookup CSV

---

## Project Structure

```
TaxiPulse_Bigdata/
├── docker/
│   ├── docker-compose.yml         # Multi-node Hadoop & Spark YARN cluster setup
│   └── spark-client/
│       └── Dockerfile              # Custom Spark client image with SBT and Scala
├── scala-pipeline/
│   ├── build.sbt                   # SBT build configuration (Scala 2.12, Spark 3.x)
│   └── src/main/scala/com/taxipulse/
│       ├── PreprocessJob.scala     # Main execution pipeline entry point
│       ├── SchemaUnifier.scala     # Standardizes multi-vendor trip schemas
│       ├── DataCleaner.scala       # Implements 13 veracity cleaning rules
│       ├── ZoneEnricher.scala      # Broadcast join with NYC Taxi Zone Lookup
│       ├── WeatherJoiner.scala     # Temporal join with Open-Meteo Weather JSON
│       ├── FeatureBuilder.scala    # Spatial, temporal, and rate feature engineering
│       ├── QualityReport.scala     # Generates data quality metrics report
│       └── Config.scala            # CLI argument parser (Scopt)
├── scripts/
│   ├── 01_download_data.py         # Automated trip dataset fetcher
│   ├── 02_download_weather.py      # Weather dataset downloader
│   ├── 03_upload_to_hdfs.ps1       # HDFS data ingestion script
│   └── 04_stage_downloaded_data.py # Local dataset staging utility
├── .gitignore                      # Git exclusion rules
└── README.md                       # Project documentation
```

---

## Quick Start Guide

### 1. Start the Distributed Cluster (Docker)
```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 2. Upload Raw Datasets to HDFS
```powershell
.\scripts\03_upload_to_hdfs.ps1
```

### 3. Build Scala Assembly JAR
```bash
docker exec spark-client bash -c "cd /workspace/scala-pipeline && sbt assembly"
```

### 4. Submit Preprocessing Job to YARN
```bash
docker exec spark-client bash -c "/opt/spark/bin/spark-submit \
  --class com.taxipulse.PreprocessJob \
  --master yarn \
  --deploy-mode client \
  --conf spark.hadoop.yarn.resourcemanager.hostname=resourcemanager \
  --driver-memory 2g \
  --executor-memory 1g \
  /workspace/scala-pipeline/target/scala-2.12/taxipulse-assembly-1.0.jar \
  --input hdfs://namenode:9000/taxipulse/uploaded \
  --output hdfs://namenode:9000/taxipulse/processed \
  --reference hdfs://namenode:9000/taxipulse/uploaded"
```

---

## Pipeline Stages & Transformations

1. **Ingestion & Schema Unification**: Standardizes disparate vendor schemas (tpep, lpep, fhvhv) into a single canonical schema (`pickup_ts`, `dropoff_ts`, `pu_location_id`, `do_location_id`, `fare_amount`, `trip_distance`).
2. **Data Veracity Cleaning**: Filters out invalid coordinates outside NYC, negative fares, impossible timestamps, zero distances, and speed outliers.
3. **Spatial & Temporal Enrichment**:
   - **Zone Lookup**: Performs broadcast joins to resolve location IDs to NYC Boroughs.
   - **Weather Integration**: Joins hourly temperature, precipitation, and weather conditions.
4. **Feature Engineering**: Computes Haversine distance, trip duration, average speed (mph), fare rate per mile, and time features (day of week, hour, peak hour indicators).
5. **Partitioned HDFS Output**: Writes compressed Snappy Parquet files partitioned by `pickup_year` and `pickup_month` to `hdfs://namenode:9000/taxipulse/processed/trips`.

---

## Cluster Web Interfaces

* **Hadoop NameNode UI**: `http://localhost:9870`
* **YARN ResourceManager UI**: `http://localhost:8088`
* **Spark Application UI**: `http://localhost:4040`

---

## License
This project is licensed under the Apache License 2.0.
