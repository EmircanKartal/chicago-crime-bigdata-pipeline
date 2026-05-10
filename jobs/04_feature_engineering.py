# Owner: Berfin
# Purpose: Build ML-ready feature table from Silver Delta layer
# Input:  delta/silver/chicago_crimes_clean
# Output: delta/gold/ml_features
#
# Target: arrest (binary classification — highest accuracy)
#
# Feature groups (14 features total, NO leakage):
#   Time        : hour, day_of_week, month, is_weekend, is_night
#   Crime       : primary_type_group, crime_group, domestic_numeric
#   Location    : district, beat, community_area, location_group
#   Geographic  : lat_grid, lon_grid_abs

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, hour, dayofweek, month,
    when, trim, upper,
    abs as spark_abs,
    round as spark_round,
    coalesce, to_timestamp, count,
)

spark = (
    SparkSession.builder
    .appName("FeatureEngineering")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_SOURCE = os.environ.get("FEATURE_DATA_SOURCE", "delta_silver").strip().lower()
CSV_PATH    = "/app/data/raw/chicago_crimes_2m.csv"
SILVER_PATH = "/app/delta/silver/chicago_crimes_clean"
OUTPUT_PATH = "/app/delta/gold/ml_features"

# Crime type groupings
VIOLENT_TYPES = [
    "BATTERY", "ASSAULT", "ROBBERY", "CRIMINAL SEXUAL ASSAULT",
    "SEX OFFENSE", "HOMICIDE", "KIDNAPPING", "STALKING",
    "INTIMIDATION", "HUMAN TRAFFICKING", "OFFENSE INVOLVING CHILDREN",
]
PROPERTY_TYPES = [
    "THEFT", "BURGLARY", "MOTOR VEHICLE THEFT", "CRIMINAL DAMAGE",
    "ARSON", "DECEPTIVE PRACTICE",
]
TOP15_TYPES = [
    "THEFT", "BATTERY", "CRIMINAL DAMAGE", "ASSAULT", "MOTOR VEHICLE THEFT",
    "OTHER OFFENSE", "DECEPTIVE PRACTICE", "BURGLARY", "NARCOTICS",
    "CRIMINAL TRESPASS", "ROBBERY", "WEAPONS VIOLATION",
    "CRIMINAL SEXUAL ASSAULT", "OFFENSE INVOLVING CHILDREN", "SEX OFFENSE",
]


def pick_col(df, candidates):
    existing = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in existing:
            return existing[name.lower()]
    raise ValueError(f"None of {candidates} found. Available: {df.columns}")


def main():
    # ── Load ─────────────────────────────────────────────────────────────
    if DATA_SOURCE == "csv":
        print(f"Reading CSV: {CSV_PATH}")
        df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
        df = df.withColumn(
            "crime_timestamp",
            coalesce(
                to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"),
            )
        )
    else:
        print(f"Reading Silver Delta: {SILVER_PATH}")
        df = spark.read.format("delta").load(SILVER_PATH)

    print(f"Raw rows: {df.count():,}")

    arrest_col       = pick_col(df, ["arrest", "Arrest"])
    domestic_col     = pick_col(df, ["domestic", "Domestic"])
    primary_type_col = pick_col(df, ["primary_type", "Primary Type"])
    location_col     = pick_col(df, ["location_description", "Location Description"])
    district_col     = pick_col(df, ["district", "District"])
    beat_col         = pick_col(df, ["beat", "Beat"])
    community_col    = pick_col(df, ["community_area", "Community Area"])
    latitude_col     = pick_col(df, ["latitude", "Latitude"])
    longitude_col    = pick_col(df, ["longitude", "Longitude"])

    # Timestamp
    ts_col = "crime_timestamp" if "crime_timestamp" in df.columns else None
    if ts_col:
        df = df.withColumnRenamed("crime_timestamp", "event_ts")
    else:
        df = df.withColumn(
            "event_ts",
            coalesce(
                to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"),
            )
        )

    # ── Feature engineering ───────────────────────────────────────────────
    feature_df = (
        df
        .withColumn("pt_upper", upper(trim(col(primary_type_col))))
        .withColumn("loc_upper", upper(trim(col(location_col))))

        # ── GROUP 1: Time ─────────────────────────────────────────────────
        .withColumn("hour",        hour(col("event_ts")))
        .withColumn("day_of_week", dayofweek(col("event_ts")))
        .withColumn("month",       month(col("event_ts")))
        .withColumn("is_weekend",  when(col("day_of_week").isin([1, 7]), lit(1)).otherwise(lit(0)))
        .withColumn("is_night",    when((col("hour") >= 22) | (col("hour") <= 5), lit(1)).otherwise(lit(0)))

        # ── GROUP 2: Crime characteristics ────────────────────────────────
        # primary_type_group: top 15 types, rest → OTHER
        # KEY for arrest: narcotics ~75% arrested, theft ~5% arrested
        .withColumn(
            "primary_type_group",
            when(col("pt_upper").isin(TOP15_TYPES), col("pt_upper"))
            .otherwise(lit("OTHER"))
        )
        # 3-class super-category
        .withColumn(
            "crime_group",
            when(col("pt_upper").isin(VIOLENT_TYPES),  lit("VIOLENT"))
            .when(col("pt_upper").isin(PROPERTY_TYPES), lit("PROPERTY"))
            .otherwise(lit("OTHER"))
        )
        # Domestic incidents → mandatory arrest in Illinois
        .withColumn(
            "domestic_numeric",
            when(col(domestic_col).cast("boolean") == True, lit(1)).otherwise(lit(0))
        )

        # ── GROUP 3: Location ─────────────────────────────────────────────
        .withColumn("district",      col(district_col).cast("integer"))
        .withColumn("beat",          col(beat_col).cast("integer"))
        .withColumn("community_area",col(community_col).cast("integer"))
        .withColumn(
            "location_group",
            when(col("loc_upper").contains("STREET"),     lit("STREET"))
            .when(col("loc_upper").contains("RESIDENCE"),  lit("RESIDENCE"))
            .when(col("loc_upper").contains("APARTMENT"),  lit("RESIDENCE"))
            .when(col("loc_upper").contains("SIDEWALK"),   lit("STREET"))
            .when(col("loc_upper").contains("PARKING"),    lit("PARKING"))
            .when(col("loc_upper").contains("STORE"),      lit("STORE"))
            .when(col("loc_upper").contains("SCHOOL"),     lit("SCHOOL"))
            .when(col("loc_upper").contains("VEHICLE"),    lit("VEHICLE"))
            .when(col("loc_upper").contains("ALLEY"),      lit("ALLEY"))
            .when(col("loc_upper").contains("RESTAURANT"), lit("RESTAURANT"))
            .otherwise(lit("OTHER"))
        )

        # ── GROUP 4: Geographic grid ──────────────────────────────────────
        # coalesce → fill missing coordinates with 0.0 so no NaN reaches models
        .withColumn("lat_grid",
            coalesce(spark_round(col(latitude_col).cast("double"), 2), lit(0.0)))
        .withColumn("lon_grid_abs",
            coalesce(spark_abs(spark_round(col(longitude_col).cast("double"), 2)), lit(0.0)))

        # ── Target ────────────────────────────────────────────────────────
        .withColumn(
            "label",
            when(col(arrest_col).cast("boolean") == True, lit(1.0)).otherwise(lit(0.0))
        )
    )

    # ── Select final columns ──────────────────────────────────────────────
    final_df = (
        feature_df
        .select(
            "event_ts",
            "label",                                          # target
            # Time
            "hour", "day_of_week", "month", "is_weekend", "is_night",
            # Crime
            "primary_type_group", "crime_group", "domestic_numeric",
            # Location
            "district", "beat", "community_area", "location_group",
            # Geographic
            "lat_grid", "lon_grid_abs",
        )
        .dropna(subset=[
            "label", "hour", "day_of_week", "month",
            "primary_type_group", "district", "location_group",
        ])
    )

    total = final_df.count()
    pos   = final_df.filter(col("label") == 1.0).count()
    neg   = total - pos
    print(f"\nFeature table rows : {total:,}")
    print(f"  Arrested   (1)   : {pos:,}  ({100*pos/total:.1f}%)")
    print(f"  Not arrested (0) : {neg:,}  ({100*neg/total:.1f}%)")
    print(f"  Features         : 14")
    print("Schema:")
    final_df.printSchema()

    print(f"\nWriting to: {OUTPUT_PATH}")
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
