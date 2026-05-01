FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    scikit-learn \
    mlflow \
    delta-spark

USER spark