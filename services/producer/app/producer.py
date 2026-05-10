# Owner: Emircan
# Branch: emircan/infra-kafka
# Purpose: Read CSV row-by-row and produce JSON messages to Kafka at configurable rate
# Input:  data/raw/chicago_crimes_sample.csv
# Output: Kafka topic chicago_crimes_raw

import csv
import json
import os
import time
import hashlib
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chicago_crimes_raw")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/raw/chicago_crimes_sample.csv")
PRODUCE_RATE_PER_SEC = int(os.getenv("PRODUCE_RATE_PER_SEC", "10"))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "1000"))
def normalize_key(key: str) -> str:

    """
    CSV kolon adlarını standart hale getirir.
    Örnek: 'Case Number' -> 'case_number'
    """

    return key.strip().lower().replace(" ", "_")

def create_synthetic_user_id(row: dict) -> str:

    """
    Veri setinde gerçek kullanıcı ID yok.
    Bu yüzden district + beat + ward bilgilerinden sentetik bir kullanıcı/aktör ID üretiyoruz.
    """
    raw_value = f"{row.get('district', '')}_{row.get('beat', '')}_{row.get('ward', '')}"
    hashed = hashlib.md5(raw_value.encode("utf-8")).hexdigest()[:10]
    return f"user_{hashed}"

def build_event(row: dict) -> dict:
    """
    Rubric'e uygun Kafka mesajını oluşturur.
    Her mesajda timestamp, kullanıcı ID, olay tipi ve ilgili ID bilgileri bulunur.
    """

    normalized = {normalize_key(k): v for k, v in row.items()}
    event = {

        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "synthetic_user_id": create_synthetic_user_id(normalized),
        "event_type": normalized.get("primary_type"),
        "primary_type": normalized.get("primary_type"),
        "related_id": normalized.get("case_number"),
        "case_number": normalized.get("case_number"),
        "crime_id": normalized.get("id"),
        "date": normalized.get("date"),
        "block": normalized.get("block"),
        "iucr": normalized.get("iucr"),
        "description": normalized.get("description"),
        "location_description": normalized.get("location_description"),
        "arrest": normalized.get("arrest"),
        "domestic": normalized.get("domestic"),
        "beat": normalized.get("beat"),
        "district": normalized.get("district"),
        "ward": normalized.get("ward"),
        "community_area": normalized.get("community_area"),
        "fbi_code": normalized.get("fbi_code"),
        "x_coordinate": normalized.get("x_coordinate"),
        "y_coordinate": normalized.get("y_coordinate"),
        "year": normalized.get("year"),
        "updated_on": normalized.get("updated_on"),
        "latitude": normalized.get("latitude"),
        "longitude": normalized.get("longitude"),
    }

    return event

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        retries=5,
    )

def stream_csv_to_kafka():

    print("[INFO] Kafka Producer starting...")
    print(f"[INFO] Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic: {KAFKA_TOPIC}")
    print(f"[INFO] CSV path: {CSV_PATH}")
    print(f"[INFO] Produce rate: {PRODUCE_RATE_PER_SEC} msg/sec")
    print(f"[INFO] Max messages: {MAX_MESSAGES}")
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    producer = create_producer()
    sleep_time = 1 / PRODUCE_RATE_PER_SEC if PRODUCE_RATE_PER_SEC > 0 else 0
    sent_count = 0
    with open(CSV_PATH, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            event = build_event(row)
            key = event.get("crime_id") or event.get("case_number") or sent_count
            producer.send(
                KAFKA_TOPIC,
                key=key,
                value=event
            )

            sent_count += 1
            if sent_count % 100 == 0:
                producer.flush()
                print(f"[INFO] {sent_count} messages sent to topic '{KAFKA_TOPIC}'")
            if sent_count >= MAX_MESSAGES:
                break
            if sleep_time > 0:
                time.sleep(sleep_time)

    producer.flush()
    producer.close()
    print(f"[SUCCESS] Producer finished. Total messages sent: {sent_count}")

if __name__ == "__main__":
    stream_csv_to_kafka()

# TODO: load CSV, iterate rows, build message dict, produce to Kafka topic
