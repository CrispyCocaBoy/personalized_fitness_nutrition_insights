#!/bin/bash

topics=(
  wearables.ppg.raw
  wearables.skin-temp.raw
  wearables.accelerometer.raw
  wearables.gyroscope.raw
  wearables.altimeter.raw
  wearables.barometer.raw
  wearables.ceda.raw
  users_changes # CDC
)

# Setting kafka
KAFKA_CONTAINER="broker_kafka"
BOOTSTRAP_SERVER="broker_kafka:9092"

# Setting topics
PARTITION="1"
REPLICATION_FACTOR="1"

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
    --partitions $PARTITION \
    --replication-factor $REPLICATION_FACTOR \
    --if-not-exists
done
echo "Sensor topic created"

