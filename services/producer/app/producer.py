# Owner: Emircan
# Branch: emircan/infra-kafka
# Purpose: Read CSV row-by-row and produce JSON messages to Kafka at configurable rate
# Input:  data/raw/chicago_crimes_sample.csv
# Output: Kafka topic chicago_crimes_raw

import os
import json
import time
import hashlib
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chicago_crimes_raw")
PRODUCE_RATE_PER_SEC = int(os.getenv("PRODUCE_RATE_PER_SEC", 10))

# TODO: load CSV, iterate rows, build message dict, produce to Kafka topic
