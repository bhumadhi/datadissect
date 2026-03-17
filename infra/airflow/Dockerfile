FROM apache/airflow:2.9.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.0.0 \
    minio==7.2.5 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark==4.7.1