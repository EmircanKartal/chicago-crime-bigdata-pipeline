# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Read streaming data from Kafka, write raw payload to Delta bronze layer
# Input:  Kafka topic chicago_crimes_raw
# Output: delta/bronze/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = "chicago_kafka:9092"
KAFKA_TOPIC = "chicago_crimes_raw"

BRONZE_PATH = "/app/delta/bronze/chicago_crimes_raw"
CHECKPOINT_PATH = "/app/delta/checkpoints/bronze_chicago_crimes_raw"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ChicagoCrimeKafkaToBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def get_crime_schema():
    return StructType([
        StructField("ingest_ts", StringType(), True),
        StructField("synthetic_user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("primary_type", StringType(), True),
        StructField("related_id", StringType(), True),
        StructField("case_number", StringType(), True),
        StructField("crime_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("block", StringType(), True),
        StructField("iucr", StringType(), True),
        StructField("description", StringType(), True),
        StructField("location_description", StringType(), True),
        StructField("arrest", StringType(), True),
        StructField("domestic", StringType(), True),
        StructField("beat", StringType(), True),
        StructField("district", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("community_area", StringType(), True),
        StructField("fbi_code", StringType(), True),
        StructField("x_coordinate", StringType(), True),
        StructField("y_coordinate", StringType(), True),
        StructField("year", StringType(), True),
        StructField("updated_on", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
    ])


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_crime_schema()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS json_value",
            "timestamp AS kafka_timestamp"
        )
        .withColumn("data", from_json(col("json_value"), schema))
        .select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("json_value"),
            col("data.*"),
            current_timestamp().alias("bronze_loaded_at")
        )
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )

    query.awaitTermination()

    print(f"[SUCCESS] Bronze Delta written to: {BRONZE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()