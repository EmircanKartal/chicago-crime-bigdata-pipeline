# Owner: Kagan
# Branch: kagan/ml-mlflow
# Purpose: Build ML feature table from silver layer
# Input:  delta/silver/
# Output: delta/gold/ml_features/
# Features: hour, day_of_week, month, is_weekend, is_night, lat_grid, lon_grid, location_group, primary_type_top10 (label)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, round as spark_round, when

spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: read silver, engineer 10 features, create label column, write to delta/gold/ml_features/
