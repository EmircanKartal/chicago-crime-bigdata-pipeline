# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Read streaming data from Kafka, write raw payload to Delta bronze layer
# Input:  Kafka topic chicago_crimes_raw
# Output: delta/bronze/

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("KafkaToBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: readStream from kafka, select key/value/offset/ingest_ts, writeStream to delta/bronze/
