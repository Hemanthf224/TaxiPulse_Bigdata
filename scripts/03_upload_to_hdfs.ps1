# scripts/03_upload_to_hdfs.ps1
# Uploads raw NYC Taxi records, zone lookups, and weather JSON to HDFS inside Docker cluster.

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "TaxiPulse - HDFS Ingestion and Directory Setup Utility" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Create HDFS Directories
Write-Host "[HDFS MKDIR] Creating HDFS directory structure..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -mkdir -p /taxipulse/raw/yellow
docker exec namenode hdfs dfs -mkdir -p /taxipulse/raw/green
docker exec namenode hdfs dfs -mkdir -p /taxipulse/raw/fhvhv
docker exec namenode hdfs dfs -mkdir -p /taxipulse/reference
docker exec namenode hdfs dfs -mkdir -p /taxipulse/processed

# 2. Upload reference files to HDFS
Write-Host "[HDFS PUT] Uploading reference zone lookup and weather JSON..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -put -f /workspace/data/reference/taxi_zone_lookup.csv /taxipulse/reference/
docker exec namenode hdfs dfs -put -f /workspace/data/reference/weather.json /taxipulse/reference/

# 3. Upload all raw trip records to HDFS
Write-Host "[HDFS PUT] Uploading staged Parquet trip records to HDFS..." -ForegroundColor Yellow

$rawTypes = @("yellow", "green", "fhvhv")
foreach ($t in $rawTypes) {
    $typeDir = "c:\TaxiPulse_Bigdata\data\raw\$t"
    if (Test-Path $typeDir) {
        $yearDirs = Get-ChildItem -Path $typeDir -Directory
        foreach ($yDir in $yearDirs) {
            $monthDirs = Get-ChildItem -Path $yDir.FullName -Directory
            foreach ($mDir in $monthDirs) {
                $hdfsDest = "/taxipulse/raw/$t/$($yDir.Name)/$($mDir.Name)"
                docker exec namenode hdfs dfs -mkdir -p $hdfsDest
                $files = Get-ChildItem -Path $mDir.FullName -Filter "*.parquet"
                foreach ($f in $files) {
                    $containerPath = "/workspace/data/raw/$t/$($yDir.Name)/$($mDir.Name)/$($f.Name)"
                    Write-Host "[HDFS PUT] Uploading $t/$($yDir.Name)/$($mDir.Name)/$($f.Name) to $hdfsDest..." -ForegroundColor Green
                    docker exec namenode hdfs dfs -D dfs.blocksize=134217728 -put -f $containerPath $hdfsDest/
                }
            }
        }
    }
}

Write-Host "[HDFS VERIFY] Verifying HDFS storage contents..." -ForegroundColor Green
docker exec namenode hdfs dfs -du -s -h /taxipulse/raw
docker exec namenode hdfs dfs -ls /taxipulse/reference

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "HDFS Ingestion Completed Successfully!" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
