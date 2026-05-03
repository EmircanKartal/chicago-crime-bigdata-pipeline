# Owner: Berfin
# Branch: berfin/spark-delta
# Purpose: Parse raw JSON from bronze, clean and type-cast, write to Delta silver layer
# Input:  delta/bronze/
# Output: delta/silver/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_timestamp, current_timestamp


BRONZE_PATH = "/app/delta/bronze/chicago_crimes_raw"
SILVER_PATH = "/app/delta/silver/chicago_crimes_clean"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ChicagoCrimeBronzeToSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    silver_df = (
        bronze_df
        .withColumn("crime_id", trim(col("crime_id")))
        .withColumn("case_number", trim(col("case_number")))
        .withColumn("primary_type", upper(trim(col("primary_type"))))
        .withColumn("location_description", upper(trim(col("location_description"))))
        .withColumn("district", trim(col("district")).cast("int"))
        .withColumn("ward", trim(col("ward")).cast("int"))
        .withColumn("community_area", trim(col("community_area")).cast("int"))
        .withColumn("beat", trim(col("beat")).cast("int"))
        .withColumn("year", trim(col("year")).cast("int"))
        .withColumn("latitude", trim(col("latitude")).cast("double"))
        .withColumn("longitude", trim(col("longitude")).cast("double"))
        .withColumn("x_coordinate", trim(col("x_coordinate")).cast("double"))
        .withColumn("y_coordinate", trim(col("y_coordinate")).cast("double"))
        .withColumn("crime_timestamp", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))        .withColumn("silver_loaded_at", current_timestamp())
    )

    before_count = silver_df.count()

    silver_df = silver_df.dropna(subset=["crime_id", "primary_type"])
    after_null_count = silver_df.count()

    silver_df = silver_df.dropDuplicates(["crime_id"])
    after_duplicate_count = silver_df.count()

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )

    print(f"[INFO] Before cleaning row count: {before_count}")
    print(f"[INFO] After null cleaning row count: {after_null_count}")
    print(f"[INFO] After duplicate cleaning row count: {after_duplicate_count}")
    print(f"[SUCCESS] Silver Delta written to: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
