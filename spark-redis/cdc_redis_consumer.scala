import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.bson.Document
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, ReplaceOptions}
import redis.clients.jedis.Jedis

object CdcRedisConsumer {

  val customerSchema: StructType = new StructType()
    .add("id",         IntegerType, nullable = true)
    .add("name",       StringType,  nullable = true)
    .add("email",      StringType,  nullable = true)
    .add("created_at", StringType,  nullable = true)

  // Wrapper ngoài: { "schema": {...}, "payload": { "op": ..., "before": ..., "after": ... } }
  val payloadInnerSchema: StructType = new StructType()
    .add("op",     StringType,     nullable = true)
    .add("before", customerSchema, nullable = true)
    .add("after",  customerSchema, nullable = true)

  val debeziumRootSchema: StructType = new StructType()
    .add("payload", payloadInnerSchema, nullable = true)

  val KAFKA_BROKERS    = "cdc-kafka:29092"
  val KAFKA_TOPIC      = "mysql.inventory.customers"
  val CHECKPOINT_PATH  = "/tmp/spark-checkpoint/cdc-customers"
  val MONGO_URI        = "mongodb://cdc-mongodb:27017"
  val MONGO_DB         = "inventory"
  val MONGO_COLLECTION = "customers"
  val REDIS_HOST       = "cdc-redis"
  val REDIS_PORT       = 6379

  def processBatch(batchDF: Dataset[Row], batchId: Long): Unit = {
    println(s"\n=== Batch $batchId (${batchDF.count()} records) ===")
    batchDF.collect().foreach { row =>
      val op    = row.getAs[String]("op")
      val id    = row.getAs[Int]("id")
      val name  = Option(row.getAs[String]("name")).getOrElse("")
      val email = Option(row.getAs[String]("email")).getOrElse("")

      val mongoClient = MongoClients.create(MONGO_URI)
      try {
        val col = mongoClient.getDatabase(MONGO_DB).getCollection(MONGO_COLLECTION)
        if (op == "d") {
          col.deleteOne(Filters.eq("_id", id))
          println(s"[MongoDB] DELETE id=$id")
        } else {
          val doc = new Document()
            .append("_id",   id)
            .append("name",  name)
            .append("email", email)
          col.replaceOne(Filters.eq("_id", id), doc, new ReplaceOptions().upsert(true))
          println(s"[MongoDB] UPSERT id=$id name=$name email=$email")
        }
      } finally { mongoClient.close() }

      val jedis = new Jedis(REDIS_HOST, REDIS_PORT)
      try {
        val key = s"customer:$id"
        if (op == "d") {
          jedis.del(key)
          println(s"[Redis] DEL $key")
        } else {
          jedis.set(key, s"""{"id":$id,"name":"$name","email":"$email"}""")
          println(s"[Redis] SET $key")
        }
      } finally { jedis.close() }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CDC-MySQL-To-Redis-MongoDB")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val parsedStream = rawStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .withColumn("root",    from_json($"json_str", debeziumRootSchema))
      .withColumn("payload", $"root.payload")
      .select(
        $"payload.op".as("op"),
        $"payload.before".as("before"),
        $"payload.after".as("after")
      )
      .withColumn("record", when($"op" === "d", $"before").otherwise($"after"))
      .filter($"record".isNotNull)
      .select(
        $"op",
        $"record.id".as("id"),
        $"record.name".as("name"),
        $"record.email".as("email")
      )

    parsedStream.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", CHECKPOINT_PATH + "/sink")
      .foreachBatch(processBatch _)
      .start()
      .awaitTermination()
  }
}

CdcRedisConsumer.main(Array.empty)
