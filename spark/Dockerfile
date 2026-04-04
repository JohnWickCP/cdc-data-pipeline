FROM apache/spark:4.0.0
USER root

RUN mkdir -p /home/spark/.ivy2 && \
    chown -R spark:spark /home/spark/.ivy2

# Spark 4.0 + Scala 2.13 JARs
RUN curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar \
         -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar \
         -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar \
         -o /opt/spark/jars/kafka-clients-3.9.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
         -o /opt/spark/jars/commons-pool2-2.12.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.13/10.5.0/mongo-spark-connector_2.13-10.5.0.jar \
         -o /opt/spark/jars/mongo-spark-connector_2.13-10.5.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.3.1/mongodb-driver-sync-5.3.1.jar \
         -o /opt/spark/jars/mongodb-driver-sync-5.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.3.1/mongodb-driver-core-5.3.1.jar \
         -o /opt/spark/jars/mongodb-driver-core-5.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/bson/5.3.1/bson-5.3.1.jar \
         -o /opt/spark/jars/bson-5.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.3.1/bson-record-codec-5.3.1.jar \
         -o /opt/spark/jars/bson-record-codec-5.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/redis/clients/jedis/5.1.0/jedis-5.1.0.jar \
         -o /opt/spark/jars/jedis-5.1.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar \
         -o /opt/spark/jars/slf4j-api-1.7.36.jar

USER root
RUN pip install --no-cache-dir redis 2>/dev/null || true
USER spark
