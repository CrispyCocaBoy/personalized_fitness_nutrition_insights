#!/bin/bash

topics=(
  wearables_bpm
  wearables_hr
  wearables_hrv
  wearables_spo2
  wearables_steps
  wearables_skin_temperature
)

KAFKA_CONTAINER="broker_kafka"
BOOTSTRAP_SERVER="broker_kafka:9092"

echo "Waiting for kafka to be ready"
while ! nc -z $KAFKA_CONTAINER 9092; do
  sleep 1
done
echo "Kafka is ready"

echo "Creating sensor topic"
for topic in "${topics[@]}"; do
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists
done
echo "Sensor topic created"

