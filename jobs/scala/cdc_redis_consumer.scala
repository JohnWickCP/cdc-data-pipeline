import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}
import org.bson.Document
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, ReplaceOptions}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import java.util.Base64
import java.math.BigInteger
import scala.jdk.CollectionConverters._

object CdcRedisConsumer {

  val recordSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("email", StringType)
    .add("phone", StringType)
    .add("customer_id", IntegerType)
    .add("total_amount", StringType)
    .add("status", StringType)

  val sourceSchema: StructType = new StructType().add("table", StringType)

  val payloadInnerSchema: StructType = new StructType()
    .add("op", StringType)
    .add("source", sourceSchema)
    .add("before", recordSchema)
    .add("after", recordSchema)

  val KAFKA_BROKERS    = "cdc-kafka:29092"   // 👈 Chỗ này của bạn chắc chắn đang bị ghi là localhost:29092
  val KAFKA_TOPICS     = "inventory.inventory.customers,inventory.inventory.orders"
  val CHECKPOINT_PATH  = "/tmp/spark-checkpoint/cdc-pipeline"
  val MONGO_URI        = "mongodb://cdc-mongodb:27017"  // 👈 Phải là cdc-mongodb
  val MONGO_DB         = "inventory"
  val REDIS_HOST       = "cdc-redis"                    // 👈 Phải là cdc-redis
  val REDIS_PORT       = 6379

  // Connection pool — tái sử dụng connection, không tạo mới mỗi partition
  @transient lazy val mongoPool: com.mongodb.client.MongoClient = 
    MongoClients.create(MONGO_URI)
  
  @transient lazy val jedisPool: redis.clients.jedis.JedisPool = {
    val config = new redis.clients.jedis.JedisPoolConfig()
    config.setMaxTotal(10)
    config.setMaxIdle(5)
    config.setMinIdle(2)
    new redis.clients.jedis.JedisPool(config, REDIS_HOST, REDIS_PORT)
  }


  val decodeDecimalUDF = udf((encoded: String) => {
    if (encoded == null) 0.0
    else {
      try {
        val bytes = Base64.getDecoder.decode(encoded)
        val intVal = new BigInteger(bytes)
        (BigDecimal(intVal) / 100).toDouble
      } catch { case _: Exception => 0.0 }
    }
  })

  val maskEmailUDF = udf((email: String) => {
    if (email == null) null
    else {
      val parts = email.split("@")
      if (parts.length != 2) email
      else {
        val local = parts(0)
        val maskedLocal =
          if (local.length > 2) local.head + "*" * (local.length - 2) + local.last
          else local
        s"$maskedLocal@${parts(1)}"
      }
    }
  })

  def processBatch(batchDF: Dataset[Row], batchId: Long): Unit = {

    if (batchDF.isEmpty) return

    println(s"\n=== Batch $batchId ===")
    val _batchStart = System.currentTimeMillis()

    batchDF.foreachPartition { partition: Iterator[Row] =>

      val mongoClient = mongoPool  // reuse pool
      val jedis = jedisPool.getResource()

      val db = mongoClient.getDatabase(MONGO_DB)
      val customersCol = db.getCollection("customers")
      val ordersCol = db.getCollection("orders")

      val pipe = jedis.pipelined()

      try {
        partition.foreach { row =>

          val op = row.getAs[String]("op")
          val table = row.getAs[String]("table")
          val id = row.getAs[Int]("id")

          if (table == "customers") {

            val name = Option(row.getAs[String]("name")).getOrElse("")
            val email = Option(row.getAs[String]("masked_email")).getOrElse("")

            if (op == "d") {
              customersCol.deleteOne(Filters.eq("_id", id))
              pipe.del(s"customer:$id")
            } else {
              val doc = new Document("_id", id)
                .append("name", name)
                .append("email", email)

              customersCol.replaceOne(
                Filters.eq("_id", id),
                doc,
                new ReplaceOptions().upsert(true)
              )

              val hashData = Map(
                "id" -> id.toString,
                "name" -> name,
                "email" -> email
              ).asJava

              pipe.hset(s"customer:$id", hashData)
              pipe.incr("customers:total")
            }

          } else if (table == "orders") {

            val customerId = row.getAs[Int]("customer_id")
            val amount = row.getAs[Double]("total_amount")
            val status = Option(row.getAs[String]("status")).getOrElse("UNKNOWN")

            if (op == "d") {
              ordersCol.deleteOne(Filters.eq("_id", id))

              pipe.decr(s"orders:status:$status")
              pipe.decr("orders:total")
              pipe.incrByFloat("orders:revenue", -amount)

            } else {

              val doc = new Document("_id", id)
                .append("customer_id", customerId)
                .append("total_amount", amount)
                .append("status", status)

              ordersCol.replaceOne(
                Filters.eq("_id", id),
                doc,
                new ReplaceOptions().upsert(true)
              )

              pipe.incr("orders:total")
              pipe.incr(s"orders:status:$status")
              pipe.incrByFloat("orders:revenue", amount)
              pipe.zincrby("top_customers:order_count", 1.0, s"customer:$customerId")
            }
          }
        }

        pipe.sync()
        val _batchEnd = System.currentTimeMillis()
        println(s"  [TIMING] total=${_batchEnd - _batchStart}ms")

      } finally {
        jedis.close()  // return to pool
        // mongoClient không close — reuse
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CDC-MySQL-To-MongoDB-Redis")
      .master("local[*]") // 🔥 FIX để chạy local
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKERS)
      .option("subscribe", KAFKA_TOPICS)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val records = rawStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .withColumn("root", from_json($"json_str", payloadInnerSchema))
      .select($"root.*")
      .withColumn("table", $"source.table")
      .withColumn("record", when($"op" === "d", $"before").otherwise($"after"))
      .filter($"record".isNotNull)
      .withColumn("masked_email", maskEmailUDF($"record.email"))
      .withColumn("amount_decoded", decodeDecimalUDF($"record.total_amount"))
      .select(
        $"op",
        $"table",
        $"record.id".as("id"),
        $"record.name".as("name"),
        $"masked_email",
        $"record.customer_id".as("customer_id"),
        $"amount_decoded".as("total_amount"),
        $"record.status".as("status")
      )

    records.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1500 milliseconds"))
      .option("checkpointLocation", CHECKPOINT_PATH)
      .foreachBatch(processBatch _)
      .start()
      .awaitTermination()
  }
}