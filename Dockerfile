# ============================================================
# AI SaaS Product Analytics Platform
# Spark Runtime
# ============================================================

FROM spark:3.5.8-python3

USER root

WORKDIR /app

# ------------------------------------------------------------
# Python Dependencies
# ------------------------------------------------------------

COPY requirements-docker.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ------------------------------------------------------------
# Kafka + Delta Lake Jars
# ------------------------------------------------------------

RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar && \
    \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar && \
    \
    curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar \
    -o /opt/spark/jars/kafka-clients-3.7.0.jar && \
    \
    curl -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    -o /opt/spark/jars/commons-pool2-2.12.0.jar && \
    \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    -o /opt/spark/jars/delta-storage-3.2.0.jar

USER spark

CMD ["tail","-f","/dev/null"]