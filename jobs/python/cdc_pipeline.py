from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from base64 import b64decode
from decimal import Decimal

# =============================
# Spark Session
# =============================

spark = SparkSession.builder \
    .appName("CDC Pipeline - MySQL to MongoDB + Redis") \
    .master("spark://cdc-spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================
# Schema Debezium (no payload wrapper - schemas.enable=false)
# =============================

record_schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("phone", StringType()) \
    .add("created_at", StringType()) \
    .add("customer_id", IntegerType()) \
    .add("total_amount", StringType()) \
    .add("status", StringType()) \
    .add("order_date", StringType())

schema = StructType() \
    .add("before", record_schema) \
    .add("after", record_schema) \
    .add("op", StringType()) \
    .add("source",
        StructType()
        .add("table", StringType())
    )

# =============================
# Decode decimal base64
# =============================

def decode_decimal(encoded):
    if encoded is None:
        return None
    try:
        bytes_val = b64decode(encoded)
        int_val = int.from_bytes(bytes_val, byteorder='big', signed=True)
        return Decimal(int_val) / Decimal(100)
    except:
        return None

decode_decimal_udf = udf(decode_decimal, DecimalType(12,2))

# =============================
# Mask email
# =============================

def mask_email(email):

    if email is None:
        return None

    parts = email.split("@")

    if len(parts) != 2:
        return email

    local = parts[0]
    domain = parts[1]

    if len(local) > 2:
        local = local[0] + "*"*(len(local)-2) + local[-1]

    return local + "@" + domain

mask_email_udf = udf(mask_email, StringType())

# =============================
# Read Kafka
# =============================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cdc-kafka:29092") \
    .option("subscribe", "inventory.inventory.customers,inventory.inventory.orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# =============================
# Parse Debezium JSON (no payload wrapper)
# =============================

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

records = parsed_df \
    .withColumn(
        "record",
        when(col("op") == "d", col("before")).otherwise(col("after"))
    ) \
    .filter(col("record").isNotNull()) \
    .withColumn("table", col("source.table")) \
    .withColumn(
        "amount_decoded",
        decode_decimal_udf(col("record.total_amount"))
    ) \
    .select(
        col("op"),
        col("table"),
        col("record.id").alias("id"),
        col("record.name").alias("name"),
        mask_email_udf(col("record.email")).alias("masked_email"),
        col("record.customer_id"),
        col("amount_decoded").alias("total_amount"),
        col("record.status").alias("status")
    )

# =============================
# Process batch
# =============================

def process_batch(batch_df, batch_id):

    count = batch_df.count()

    if count == 0:
        return

    print(f"Batch {batch_id} records: {count}")

    customers_df = batch_df \
        .filter(col("table") == "customers") \
        .select("id", "name", "masked_email") \
        .withColumn("_id", col("id"))

    orders_df = batch_df \
        .filter(col("table") == "orders") \
        .select("id", "customer_id", "total_amount", "status") \
        .withColumn("_id", col("id"))

    # =============================
    # MongoDB
    # =============================

    if not customers_df.rdd.isEmpty():

        customers_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("connection.uri", "mongodb://cdc-mongodb:27017") \
            .option("database", "inventory") \
            .option("collection", "customers") \
            .option("operationType", "replace") \
            .save()

    if not orders_df.rdd.isEmpty():

        orders_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("connection.uri", "mongodb://cdc-mongodb:27017") \
            .option("database", "inventory") \
            .option("collection", "orders") \
            .option("operationType", "replace") \
            .save()

    # =============================
    # Redis update
    # =============================

    def update_redis(partition):

        from redis import Redis

        r = Redis(host="cdc-redis", port=6379, decode_responses=True)
        pipe = r.pipeline()

        for row in partition:

            op = row["op"]
            table = row["table"]
            id_val = row["id"]

            if table == "customers":

                key = f"customer:{id_val}"

                if op == "d":
                    pipe.delete(key)
                else:
                    pipe.hset(key, mapping={
                        "id": str(id_val),
                        "name": row["name"] or "",
                        "email": row["masked_email"] or ""
                    })

                pipe.incr("customers:total")

            elif table == "orders":

                status = row["status"]
                amount = float(row["total_amount"] or 0)

                if op == "d":

                    if status:
                        pipe.decr(f"orders:status:{status}")

                    pipe.decr("orders:total")
                    pipe.decrbyfloat("orders:revenue", amount)

                else:

                    pipe.incr("orders:total")

                    if status:
                        pipe.incr(f"orders:status:{status}")

                    pipe.incrbyfloat("orders:revenue", amount)

                    pipe.zincrby(
                        "top_customers:order_count",
                        1,
                        f"customer:{row['customer_id']}"
                    )

        pipe.execute()

    batch_df.foreachPartition(update_redis)

# =============================
# Start streaming
# =============================

query = records.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoint/cdc-pipeline") \
    .start()

query.awaitTermination()