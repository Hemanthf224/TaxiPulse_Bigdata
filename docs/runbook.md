# TaxiPulse — Operational Runbook

This runbook provides the exact, step-by-step commands to reproduce the entire **TaxiPulse** Big Data ingestion, HDFS storage, and Scala Spark preprocessing pipeline from scratch.

---

## 1. Environment & Prerequisites Verification

Ensure the following tools are installed on your machine:
- **Docker Desktop** (with WSL2 engine enabled)
- **PowerShell** (for automation scripts)
- **Python 3.10+** (with `pandas`, `pyarrow`, `requests`)

Verify free disk space (minimum 70–90 GB required for full multi-year data):
```powershell
Get-PSDrive -PSProvider FileSystem
```

---

## 2. Bring Up the 3-DataNode Hadoop Cluster (Phase 0)

Navigate to the `docker/` directory and launch the containers:
```powershell
cd c:\TaxiPulse_Bigdata\docker
docker compose up -d --build
```

Verify that all services are healthy and that **3 Live DataNodes** are active:
```powershell
docker exec namenode hdfs dfsadmin -report
```
- **NameNode UI**: `http://localhost:9870`
- **YARN Resource Manager UI**: `http://localhost:8088`

---

## 3. Data Download & Staging (Phase 1.1)

To download official NYC TLC trip records and weather data:
```powershell
cd c:\TaxiPulse_Bigdata
& "C:\Users\M.Hemanth reddy\anaconda3\python.exe" scripts/01_download_data.py --year 2024 --months 01 --types yellow,green,fhvhv
& "C:\Users\M.Hemanth reddy\anaconda3\python.exe" scripts/02_download_weather.py --start-date 2022-01-01 --end-date 2024-12-31
```

If dataset is already downloaded at `C:\TaxiPulse_Data\`:
```powershell
& "C:\Users\M.Hemanth reddy\anaconda3\python.exe" scripts/04_stage_downloaded_data.py
```

---

## 4. Ingest Raw Datasets into HDFS (Phase 1.2 — Demo Requirement #2)

Upload raw Parquet files, Taxi Zone lookup table, and weather JSON to HDFS:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/03_upload_to_hdfs.ps1
```

Verify HDFS storage directory layout:
```powershell
docker exec namenode hdfs dfs -ls /taxipulse/raw
docker exec namenode hdfs dfs -ls /taxipulse/reference
```

---

## 5. Compile and Submit Scala Preprocessing Job (Phase 1.3 — Demo Requirements #3, #4, #5, #6)

### Compile Scala Assembly Fat JAR
```powershell
docker exec spark-client bash -c "cd /workspace/scala-pipeline && sbt assembly"
```

### Submit Job to YARN Cluster
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

---

## 6. Verify Processed Data & Capture Evidence (Phase 1.6 — Demo Requirement #7)

Verify processed Parquet dataset written back to HDFS:
```powershell
docker exec namenode hdfs dfs -du -s -h /taxipulse/processed/trips
```

Capture evidence logs for presentation review:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/99_capture_evidence.ps1
```

Review generated evidence files in `docs/evidence/phase1/` and data quality report in `docs/data_quality_report.md`.
