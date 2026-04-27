# Owner: Kagan
# Branch: kagan/ml-mlflow
# Purpose: Train 5 classification models, track all runs in MLflow
# Input:  delta/gold/ml_features/
# Output: MLflow runs at http://mlflow:5000, best model artifact
# Models: LogisticRegression, DecisionTree, RandomForest, GBT (binary), NaiveBayes

from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, NaiveBayes
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import mlflow
import mlflow.spark

spark = SparkSession.builder.appName("TrainModels").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

MLFLOW_TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "chicago_crime_classification"

# TODO: read ml_features, define pipeline, train each model, log params/metrics/model to MLflow
