# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Aggregate silver data into EDA gold tables
# Input:  delta/silver/
# Output: delta/gold/crime_by_type/, delta/gold/crime_hourly/, delta/gold/crime_by_district/, delta/gold/crime_daily/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, when, current_timestamp, lower, trim


SILVER_PATH = "/app/delta/silver/chicago_crimes_clean"
GOLD_PATH = "/app/delta/gold/chicago_crimes_features"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ChicagoCrimeSilverToGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(SILVER_PATH)

    gold_df = (
        silver_df
        .withColumn("crime_hour", hour(col("crime_timestamp")))
        .withColumn("crime_day_of_week", dayofweek(col("crime_timestamp")))
        .withColumn("crime_month", month(col("crime_timestamp")))
        .withColumn("is_weekend", when(col("crime_day_of_week").isin([1, 7]), 1).otherwise(0))
        .withColumn("is_night", when((col("crime_hour") >= 22) | (col("crime_hour") <= 5), 1).otherwise(0))
        .withColumn("arrest_int", when(lower(trim(col("arrest"))) == "true", 1).otherwise(0))
        .withColumn("domestic_int", when(lower(trim(col("domestic"))) == "true", 1).otherwise(0))
        .withColumn("gold_loaded_at", current_timestamp())
    )

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )

    print(f"[INFO] Gold row count: {gold_df.count()}")
    print(f"[SUCCESS] Gold Delta written to: {GOLD_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
