# 05_train_ml_model.ps1
# TaxiPulse — Submit Distributed Spark MLlib Training Job to Hadoop YARN Cluster

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "TaxiPulse — Submitting Spark MLlib Model Training to Hadoop YARN" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# Submit ML Job to Spark YARN Cluster
docker exec spark-client bash -c "/opt/spark/bin/spark-submit \
  --class com.taxipulse.TrainMLModel \
  --master yarn \
  --deploy-mode client \
  --conf spark.hadoop.yarn.resourcemanager.hostname=resourcemanager \
  --driver-memory 2g \
  --executor-memory 1g \
  /workspace/scala-pipeline/target/scala-2.12/taxipulse-assembly-1.0.jar \
  --input hdfs://namenode:9000/taxipulse/processed/trips \
  --output hdfs://namenode:9000/taxipulse/models"

Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host "[SUCCESS] ML Training job completed!" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
