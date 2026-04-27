#!/bin/bash
docker compose exec kafka kafka-topics \
  --create \
  --topic chicago_crimes_raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
