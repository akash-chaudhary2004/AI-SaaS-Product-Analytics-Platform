Write-Host "========================================="
Write-Host "Starting AI SaaS Product Analytics Platform"
Write-Host "========================================="

docker compose up -d

Write-Host ""
Write-Host "Checking Kafka topic..."

$topics = docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

if ($topics -match "ai-product-events") {
    Write-Host "Topic already exists."
}
else {
    Write-Host "Creating topic..."

    docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create `
        --topic ai-product-events `
        --partitions 1 `
        --replication-factor 1

    Write-Host "Topic created."
}

Write-Host ""
Write-Host "Project is ready."