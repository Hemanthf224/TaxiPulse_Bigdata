# TaxiPulse — First Review Live Demonstration Script

This script outlines the exact 15-minute live demonstration workflow for the **23AID302 Big Data Analytics First Review**.

---

## 🕒 Step 1: Demonstrate Hadoop Multi-Node Cluster (Demo Requirement #1)

Open PowerShell and execute:
```powershell
docker exec namenode hdfs dfsadmin -report
```
**What to highlight during viva:**
- Point out **Live DataNodes (3)**: `datanode1`, `datanode2`, `datanode3`.
- Open NameNode UI in browser: `http://localhost:9870` → *Datanodes* tab to show active nodes and total capacity (~2.95 TB).
- Open YARN UI in browser: `http://localhost:8088` to show active NodeManager.

---

## 🕒 Step 2: Demonstrate Raw Dataset in HDFS (Demo Requirement #2)

Execute:
```powershell
docker exec namenode hdfs dfs -du -s -h /taxipulse/raw/*
docker exec namenode hdfs dfs -ls /taxipulse/reference
```
**What to highlight during viva:**
- Show raw trip Parquet files stored in HDFS (`/taxipulse/raw/yellow`, `/taxipulse/raw/green`, `/taxipulse/raw/fhvhv`).
- Show semi-structured Open-Meteo weather JSON (`/taxipulse/reference/weather.json`) and spatial Taxi Zone lookup (`/taxipulse/reference/taxi_zone_lookup.csv`).
- Demonstrate block distribution and replication:
```powershell
docker exec namenode hdfs fsck /taxipulse/raw -files -blocks -locations
```
- Point out replication factor `2` across the 3 DataNodes.

---

## 🕒 Step 3: Demonstrate Scala Preprocessing Code & Execution (Demo Requirements #3 & #4)

Show source code in IDE or terminal:
- `Config.scala`: CLI options parser.
- `SchemaUnifier.scala`: Unifies schemas across Yellow, Green, and FHVHV into a single canonical format.
- `DataCleaner.scala`: Implements **13 explicit veracity rules** (negative fares, zero distance, 0 passengers, speed anomalies, timestamp inversions).
- `ZoneEnricher.scala`: Uses `broadcast()` join for 265 taxi zones to prevent unnecessary network shuffle.
- `WeatherJoiner.scala`: Joins Open-Meteo hourly weather JSON on pickup timestamp.
- `FeatureBuilder.scala`: Computes `trip_duration_min`, `avg_speed_mph`, `pickup_hour`, `is_weekend`, `is_night`, `is_airport_trip`, `fare_per_mile`.

Submit the compiled Scala fat JAR to YARN:
```powershell
docker exec spark-client spark-submit \
  --master yarn \
  --deploy-mode client \
  --class com.taxipulse.PreprocessJob \
  --conf spark.sql.shuffle.partitions=200 \
  /workspace/scala-pipeline/target/scala-2.12/taxipulse-assembly-1.0.jar \
  --input hdfs://namenode:9000/taxipulse/raw \
  --output hdfs://namenode:9000/taxipulse/processed \
  --reference hdfs://namenode:9000/taxipulse/reference
```
**What to highlight during viva:**
- Point to the YARN UI (`http://localhost:8088`) showing application state **RUNNING** → **SUCCEEDED**.
- Point to Spark UI (`http://localhost:4040`) showing stage execution DAG and task distribution.

---

## 🕒 Step 4: Demonstrate Processed Dataset Stored in HDFS (Demo Requirements #5 & #6)

Execute:
```powershell
docker exec namenode hdfs dfs -du -s -h /taxipulse/processed/trips
docker exec namenode hdfs dfs -ls /taxipulse/processed/trips/pickup_year=2024
```
**What to highlight during viva:**
- Cleaned dataset is stored back in HDFS in snappy-compressed, partitioned Parquet format (`year=2024/month=01/`).
- Show the generated Data Quality Report: `docs/data_quality_report.md` detailing the before-vs-after record counts and row removal breakdown per cleaning rule.

---

## 🕒 Step 5: Live DataNode Failover Demonstration (Winning Viva Moment)

To prove genuine multi-node fault tolerance to reviewers:
1. Stop `datanode3`:
```powershell
docker stop datanode3
```
2. Re-run `hdfs dfsadmin -report` to show cluster updated to 2 live nodes:
```powershell
docker exec namenode hdfs dfsadmin -report
```
3. Read data from HDFS seamlessly to prove block replication factor 2 kept data 100% accessible:
```powershell
docker exec namenode hdfs dfs -text /taxipulse/reference/taxi_zone_lookup.csv | head -n 10
```
4. Restart `datanode3`:
```powershell
docker start datanode3
```
