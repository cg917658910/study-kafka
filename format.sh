#!/bin/bash

if [ ! -f /kafka/kraft-combined-logs/meta.properties ]; then
  echo "First time format: generating Cluster ID..."
  export CLUSTER_ID=$(/opt/kafka/bin/kafka-storage random-uuid)
  echo "Cluster ID: $CLUSTER_ID"
  /opt/kafka/bin/kafka-storage format -t $CLUSTER_ID -c /etc/kafka/kafka.properties
else
  echo "Kafka log directory already formatted. Skipping format step."
fi
