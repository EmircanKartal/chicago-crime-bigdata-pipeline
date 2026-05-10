FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    scikit-learn \
    mlflow \
    "delta-spark==3.1.0"

# --- Delta Lake 3.1.0 (compatible with Spark 3.5.1 / Scala 2.12) ---
RUN curl -fL \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar" \
    -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar && \
    curl -fL \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar" \
    -o /opt/spark/jars/delta-storage-3.1.0.jar

# --- Spark-Kafka connector + dependencies (Structured Streaming ↔ Kafka) ---
# spark-sql-kafka-0-10 is NOT bundled with apache/spark image — must be added manually.
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
    -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -fL \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
    -o /opt/spark/jars/commons-pool2-2.12.0.jar

# Give the spark user a writable ivy2 home (needed if any job still triggers ivy)
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2

USER spark