# Owner: Berfin
# Branch: feature/step5-step6-ml
# Purpose: Build ML feature table from silver layer
# Input:  delta/silver/
# Output: delta/gold/ml_features/
# Target: Arrest prediction, binary classification
# Features:
#   hour, day_of_week, month, is_weekend, is_night,
#   domestic_numeric, lat_grid, lon_grid_abs, geo_available,
#   primary_type_group, location_group, district_group

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    hour,
    dayofweek,
    month,
    when,
    trim,
    upper,
    abs as spark_abs,
    round as spark_round,
    coalesce,
    to_timestamp,
    count,
)

spark = (
    SparkSession.builder
    .appName("FeatureEngineering")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "delta/silver"
OUTPUT_PATH = "delta/gold/ml_features"


def pick_col(df, candidates):
    """
    Finds a column in the DataFrame by trying multiple possible names.
    This makes the script robust against naming differences such as
    'Primary Type' vs 'primary_type'.
    """
    existing = {c.lower(): c for c in df.columns}

    for name in candidates:
        if name.lower() in existing:
            return existing[name.lower()]

    raise ValueError(
        f"None of these columns were found: {candidates}\n"
        f"Available columns: {df.columns}"
    )


def main():
    print(f"Reading silver Delta table from: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)

    print("Silver schema:")
    df.printSchema()

    date_col = pick_col(df, ["date", "Date", "event_time", "timestamp"])
    arrest_col = pick_col(df, ["arrest", "Arrest"])
    domestic_col = pick_col(df, ["domestic", "Domestic"])
    primary_type_col = pick_col(df, ["primary_type", "Primary Type"])
    location_col = pick_col(df, ["location_description", "Location Description"])
    district_col = pick_col(df, ["district", "District"])
    latitude_col = pick_col(df, ["latitude", "Latitude"])
    longitude_col = pick_col(df, ["longitude", "Longitude"])

    # Convert Date column to timestamp.
    # Chicago Crime data usually uses a format similar to MM/dd/yyyy hh:mm:ss a.
    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col(date_col)),
            to_timestamp(col(date_col), "MM/dd/yyyy hh:mm:ss a"),
            to_timestamp(col(date_col), "yyyy-MM-dd HH:mm:ss"),
        )
    )

    # Keep top 10 primary crime types and group the rest as OTHER.
    top_primary_types = (
        df.groupBy(primary_type_col)
        .agg(count("*").alias("cnt"))
        .orderBy(col("cnt").desc())
        .limit(10)
        .select(primary_type_col)
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    print("Top 10 primary crime types:")
    for item in top_primary_types:
        print(f"- {item}")

    feature_df = (
        df
        .withColumn("primary_type_clean", upper(trim(col(primary_type_col))))
        .withColumn("location_clean", upper(trim(col(location_col))))
        .withColumn("district_clean", trim(col(district_col).cast("string")))
        .withColumn(
            "primary_type_group",
            when(col(primary_type_col).isin(top_primary_types), col(primary_type_col))
            .otherwise(lit("OTHER"))
        )
        .withColumn(
            "location_group",
            when(col("location_clean").contains("STREET"), lit("STREET"))
            .when(col("location_clean").contains("RESIDENCE"), lit("RESIDENCE"))
            .when(col("location_clean").contains("APARTMENT"), lit("RESIDENCE"))
            .when(col("location_clean").contains("SIDEWALK"), lit("STREET"))
            .when(col("location_clean").contains("PARKING"), lit("PARKING"))
            .when(col("location_clean").contains("STORE"), lit("STORE"))
            .when(col("location_clean").contains("SCHOOL"), lit("SCHOOL"))
            .when(col("location_clean").contains("VEHICLE"), lit("VEHICLE"))
            .otherwise(lit("OTHER"))
        )
        .withColumn(
            "district_group",
            when(col("district_clean").isNull() | (col("district_clean") == ""), lit("UNKNOWN"))
            .otherwise(col("district_clean"))
        )
        .withColumn("hour", hour(col("event_ts")))
        .withColumn("day_of_week", dayofweek(col("event_ts")))
        .withColumn("month", month(col("event_ts")))
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), lit(1)).otherwise(lit(0)))
        .withColumn("is_night", when((col("hour") >= 22) | (col("hour") <= 5), lit(1)).otherwise(lit(0)))
        .withColumn("domestic_numeric", when(col(domestic_col).cast("boolean") == True, lit(1)).otherwise(lit(0)))
        .withColumn("lat_grid", spark_round(col(latitude_col).cast("double"), 2))
        .withColumn("lon_grid_abs", spark_abs(spark_round(col(longitude_col).cast("double"), 2)))
        .withColumn(
            "geo_available",
            when(col(latitude_col).isNotNull() & col(longitude_col).isNotNull(), lit(1)).otherwise(lit(0))
        )
        .withColumn("label", when(col(arrest_col).cast("boolean") == True, lit(1.0)).otherwise(lit(0.0)))
    )

    final_df = (
        feature_df
        .select(
            "event_ts",
            "label",
            "hour",
            "day_of_week",
            "month",
            "is_weekend",
            "is_night",
            "domestic_numeric",
            "lat_grid",
            "lon_grid_abs",
            "geo_available",
            "primary_type_group",
            "location_group",
            "district_group",
        )
        .dropna(subset=[
            "label",
            "hour",
            "day_of_week",
            "month",
            "lat_grid",
            "lon_grid_abs",
            "primary_type_group",
            "location_group",
            "district_group",
        ])
    )

    print("Feature table schema:")
    final_df.printSchema()

    total_count = final_df.count()
    print(f"Feature row count: {total_count}")

    print(f"Writing ML feature table to: {OUTPUT_PATH}")
    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(OUTPUT_PATH)
    )

    print("Feature engineering completed successfully.")


if __name__ == "__main__":
    main()