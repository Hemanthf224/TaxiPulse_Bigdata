Write-Host "Waiting for Docker Desktop to boot..."
while ($true) {
    docker info *>$null
    if ($?) { break }
    Start-Sleep -Seconds 5
}
Write-Host "Docker is up! Starting Hadoop/Spark/Kafka cluster..."
docker-compose up -d

Write-Host "Waiting for spark-client container to become ready..."
Start-Sleep -Seconds 15

Write-Host "Starting Spark Stream Processor for Web UI..."
docker exec spark-client bash -c "/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --class com.taxipulse.StreamProcessor --master local[*] /workspace/scala-pipeline/target/scala-2.12/taxipulse-assembly-1.0.jar"
