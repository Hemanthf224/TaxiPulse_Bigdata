# TaxiPulse: Big Data Scalable Taxi Demand & Analytics Pipeline

## Overview

TaxiPulse is a distributed, high-performance Big Data analytics pipeline designed to process massive-scale transportation datasets. Built entirely on Apache Spark (Scala) and running on a multi-node Hadoop YARN cluster, this architecture seamlessly handles over 16.8 GB (150+ Million rows) of data.

The pipeline unifies, cleans, and enriches multi-year NYC Taxi and Ride-hailing datasets (Yellow Taxi, Green Taxi, FHVHV / Uber & Lyft), fusing them with live Kafka streams, hourly meteorological weather data, and geographical zone lookups.

---

## Architecture Design

```text
+-----------------------------------------------------------------------------------+
|                                 TaxiPulse Core Pipeline                           |
+-----------------------------------------------------------------------------------+
|  Web Dashboard:         React + Vite + Recharts (Live Visualization)              |
|  Streaming Layer:       Apache Kafka (Real-time Trip Ingestion)                   |
|  Processing Engine:     Apache Spark 3.x (Scala DataFrames & SparkML)             |
|  Resource Manager:      Hadoop YARN (ResourceManager + 3 NodeManagers)            |
|  Storage Layer:         Hadoop HDFS (Replication Factor = 3)                      |
+-----------------------------------------------------------------------------------+
```

### Core Components
* **Hadoop HDFS**: Stores multi-gigabyte raw input trip files and partitioned Snappy Parquet outputs across distributed DataNodes.
* **Hadoop YARN**: Handles dynamic resource allocation, memory management, and distributed executor scheduling (configured for high-memory 3GB executors).
* **Apache Spark & SparkML (Scala)**: Performs schema unification, executes 13 veracity cleaning rules, spatial/temporal feature engineering, and trains deep Gradient Boosted Trees (GBTs) for demand prediction.
* **Apache Kafka**: Simulates real-time live trip ingestion streams.

### Data Sources
* Yellow & Green Taxi Parquet Datasets (2022–2024)
* FHVHV High-Volume For-Hire Vehicle Datasets (Uber/Lyft)
* Open-Meteo Weather JSON Dataset
* NYC Taxi Zone Lookup CSV

---

## Pipeline Stages & Transformations

1. **Ingestion & Schema Unification**: Standardizes disparate vendor schemas (tpep, lpep, fhvhv) into a single canonical schema.
2. **Data Veracity Cleaning**: Filters out invalid coordinates outside NYC, negative fares, impossible timestamps, zero distances, and speed outliers.
3. **Spatial & Temporal Enrichment**:
   - Zone Lookup: Broadcast joins to resolve location IDs to NYC Boroughs.
   - Weather Integration: Temporal joins with hourly temperature, precipitation, and conditions.
4. **Feature Engineering**: Computes Haversine distance, trip duration, average speed (mph), fare rate per mile, and time features (day of week, peak hours).
5. **Machine Learning Model**: Trains scalable SparkML Pipelines using VectorAssembler and GBTRegressor to predict fare amounts and trip durations.
6. **Partitioned HDFS Output**: Writes ultra-compressed Snappy Parquet files partitioned by pickup_year and pickup_month.

---

## Quick Start Guide

### 1. Start the Distributed Cluster
Brings up the HDFS NameNode, DataNodes, YARN ResourceManager, Kafka broker, and Spark Client via Docker Compose.
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

### 4. Submit Massive Job to YARN
```bash
docker exec spark-client bash -c "/opt/spark/bin/spark-submit \
  --class com.taxipulse.PreprocessJob \
  --master yarn \
  --deploy-mode client \
  --conf spark.hadoop.yarn.resourcemanager.hostname=resourcemanager \
  --driver-memory 3g \
  --executor-memory 3g \
  --conf spark.yarn.executor.memoryOverhead=512m \
  /workspace/scala-pipeline/target/scala-2.12/taxipulse-assembly-1.0.jar \
  --input hdfs://namenode:9000/taxipulse/raw \
  --output hdfs://namenode:9000/taxipulse/processed/trips_full \
  --reference hdfs://namenode:9000/taxipulse/reference"
```

### 5. Launch React Dashboard
```bash
cd frontend
npm install
npm run dev
```

---

## Cluster Web Interfaces

* Hadoop NameNode UI: http://localhost:9870
* YARN ResourceManager UI: http://localhost:8088
* Spark Application UI: http://localhost:4040
* TaxiPulse Dashboard: http://localhost:5173

---

## License
This project is licensed under the Apache License 2.0.