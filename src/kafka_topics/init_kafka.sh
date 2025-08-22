#!/bin/bash

cold_topics=(
  users_changes # CDC
  silver_layer
  gold_layer
  meals
)

hot_topics=(
  wearables.ppg.raw
  wearables.skin-temp.raw
  wearables.accelerometer.raw
  wearables.gyroscope.raw
  wearables.altimeter.raw
  wearables.barometer.raw
  wearables.ceda.raw
)


# Setting kafka
KAFKA_CONTAINER="broker_kafka"
BOOTSTRAP_SERVER="broker_kafka:9092"

# Setting topics
PARTITION="6"
REPLICATION_FACTOR="1"

echo "Waiting for kafka to be ready"
while ! nc -z $KAFKA_CONTAINER 9092; do
  sleep 1
done
echo "Kafka is ready"

# --- Cold topics ---
echo "Creating cold topics"
for topic in "${cold_topics[@]}"; do
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$topic" \
    --partitions 6 \
    --replication-factor 1 \
    --if-not-exists
done
echo "Cold topics created"

# --- Hot topics ---
echo "Creating hot topics"
for topic in "${hot_topics[@]}"; do
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$topic" \
    --partitions 12 \
    --replication-factor 1 \
    --if-not-exists
done
echo "Hot topics created"

