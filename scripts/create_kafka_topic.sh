#!/bin/bash

echo "=========================================="
echo "Checking Kafka Topic..."
echo "=========================================="

TOPIC="ai-product-events"

if /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list | grep -w "$TOPIC" >/dev/null
then
    echo "Topic already exists."
else
    echo "Creating topic..."

    /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1

    echo "Topic created."
fi

echo "=========================================="
echo "Kafka Ready"
echo "=========================================="