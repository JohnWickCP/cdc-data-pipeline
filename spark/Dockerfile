FROM apache/spark:3.5.0

USER root

# Tạo ivy cache chuẩn
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R spark:spark /home/spark/.ivy2

# Download JAR dependencies vào /opt/spark/jars/
RUN curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
         -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
         -o /opt/spark/jars/kafka-clients-3.4.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
         -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
         -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar \
         -o /opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.0/mongodb-driver-sync-4.11.0.jar \
         -o /opt/spark/jars/mongodb-driver-sync-4.11.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.0/mongodb-driver-core-4.11.0.jar \
         -o /opt/spark/jars/mongodb-driver-core-4.11.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/bson/4.11.0/bson-4.11.0.jar \
         -o /opt/spark/jars/bson-4.11.0.jar && \
    curl -fL https://repo1.maven.org/maven2/redis/clients/jedis/5.1.0/jedis-5.1.0.jar \
         -o /opt/spark/jars/jedis-5.1.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar \
         -o /opt/spark/jars/slf4j-api-1.7.36.jar

USER root
RUN pip install --no-cache-dir redis
USER spark

USER spark