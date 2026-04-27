# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Parse raw JSON from bronze, clean and type-cast, write to Delta silver layer
# Input:  delta/bronze/
# Output: delta/silver/

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: read bronze, define schema, parse JSON, drop nulls, dedup by id, write to silver
