# scripts/99_capture_evidence.ps1
# Automates evidence capture for First Review Demonstration Requirement #8

$evidenceDir = "c:\TaxiPulse_Bigdata\docs\evidence\phase1"
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "TaxiPulse — Evidence Capture Utility for Review Demonstration" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Capture HDFS DataNodes Report
Write-Host "[EVIDENCE 01] Capturing HDFS Live DataNodes report..." -ForegroundColor Yellow
docker exec namenode hdfs dfsadmin -report > "$evidenceDir\01_cluster_nodes_report.txt"

# 2. Capture HDFS Raw Storage Directory Listing
Write-Host "[EVIDENCE 02] Capturing HDFS raw dataset storage tree..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -du -s -h /taxipulse/raw/* > "$evidenceDir\02_raw_hdfs_storage.txt"

# 3. Capture FSCK Block Distribution & Replication Proof
Write-Host "[EVIDENCE 03] Capturing FSCK block locations and replication proof..." -ForegroundColor Yellow
docker exec namenode hdfs fsck /taxipulse/raw -files -blocks -locations > "$evidenceDir\03_fsck_raw.txt"

# 4. Capture Scala Code Listing
Write-Host "[EVIDENCE 04] Capturing Scala Preprocessing source code listing..." -ForegroundColor Yellow
Get-Content -Path "c:\TaxiPulse_Bigdata\scala-pipeline\src\main\scala\com\taxipulse\*.scala" > "$evidenceDir\04_scala_pipeline_code.txt"

# 5. Capture Processed Dataset HDFS Storage
Write-Host "[EVIDENCE 07] Capturing Processed dataset HDFS storage listing..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -du -s -h /taxipulse/processed/* > "$evidenceDir\07_processed_hdfs_storage.txt"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Evidence logs captured successfully in $evidenceDir" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
