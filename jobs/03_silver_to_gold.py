# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Aggregate silver data into EDA gold tables
# Input:  delta/silver/
# Output: delta/gold/crime_by_type/, delta/gold/crime_hourly/, delta/gold/crime_by_district/, delta/gold/crime_daily/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, count

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: read silver, produce 4 aggregate tables, write each to delta/gold/
