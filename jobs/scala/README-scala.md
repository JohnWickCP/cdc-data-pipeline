# Spark Structured Streaming – CDC Consumer to Redis

Module này tiêu thụ các sự kiện CDC từ Debezium trên Kafka, xử lý chúng bằng Apache Spark Structured Streaming, và ghi dữ liệu vào Redis theo real-time.

## Vị Trí Kiến Trúc

```
MySQL → Debezium → Kafka → Spark (module này) → Redis
```

## Yêu Cầu Tiên Quyết

- Docker và Docker Compose được cài đặt
- Các dịch vụ Docker chạy với `docker-compose.yml`
- Spark 3.5.0
- Kafka client phiên bản 3.4.1
- Jedis library (Redis Java client) 4.4.3
- Redis server chạy

## Bắt Đầu

### 1. Khởi Động Spark Cluster và Redis

Đảm bảo các dịch vụ Docker đang chạy:

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

Kiểm tra tất cả các container đang chạy:

```bash
docker ps
```

Truy cập Giao diện Spark Master tại: http://localhost:8080

### 2. Vào Container Spark Master

```bash
docker exec -it cdc-spark-master bash
```

### 3. Khởi Động Spark Shell với Kafka và Jedis Package

Chạy Spark Shell với các phụ thuộc Kafka và Redis cần thiết:

```bash
/opt/spark/bin/spark-shell \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,redis.clients:jedis:4.4.3
```

**Lưu ý:** Khi paste code vào spark-shell, luôn sử dụng `:paste` mode để tránh lỗi syntax.

### 4. Chạy Spark Streaming Job - Ghi vào Redis

Sử dụng `:paste` mode để paste code:

```
:paste
```

Rồi dán code này:

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis
import scala.jdk.CollectionConverters._

val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "cdc-kafka:29092")
  .option("subscribe", "mysql.inventory.customers")
  .option("startingOffsets", "latest")
  .load()

val schema = new StructType()
  .add("before", new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("email", StringType)
    .add("created_at", StringType)
  )
  .add("after", new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("email", StringType)
    .add("created_at", StringType)
  )
  .add("op", StringType)

val parsedDF = df.selectExpr("CAST(value AS STRING) as json")
  .select(get_json_object(col("json"), "$.payload").alias("payload"))
  .select(from_json(col("payload"), schema).alias("data"))
  .select("data.after.*")

val query = parsedDF.writeStream
  .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
    println(s"\n=== Batch $batchId ===")
    batchDF.foreach { row =>
      val jedis = new Jedis("cdc-redis", 6379)
      try {
        val id = row.getAs[Int]("id")
        val name = row.getAs[String]("name")
        val email = row.getAs[String]("email")
        val data = Map("id" -> id.toString, "name" -> name, "email" -> email).asJava
        jedis.hset(s"customer:$id", data)
        println(s"✓ Redis HSET customer:$id -> $name")
      } catch {
        case e: Exception => println(s"✗ Error: ${e.getMessage}")
      } finally {
        jedis.close()
      }
    }
  }
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
  .start()

query.awaitTermination()
```

Nhấn `Ctrl + D` để kết thúc `:paste` mode.

### 5. Kiểm Tra Dữ Liệu trong Redis

Mở một terminal khác và truy cập Redis CLI:

```bash
docker exec -it cdc-redis redis-cli
```

Kiểm tra tất cả keys:

```redis
KEYS customer:*
```

Xem chi tiết customer:

```redis
HGETALL customer:1
```

Đếm số customer:

```redis
DBSIZE
```

Thoát Redis CLI:

```redis
EXIT
```

### 6. Kiểm Tra CDC Theo Thời Gian Thực

Mở một terminal khác và chèn dữ liệu kiểm tra:

```bash
docker exec -it cdc-mysql mysql -uroot -proot -e \
"USE inventory; INSERT INTO customers (name, email) VALUES ('TestUser', 'test@example.com');"
```

Bạn sẽ thấy:
- Dòng `=== Batch X ===` xuất hiện trong Spark console
- Dòng `✓ Redis HSET customer:X -> TestUser` xác nhận ghi thành công
- Data mới xuất hiện trong Redis CLI

### 7. Dừng Streaming

Nhấn:

```
Ctrl + C
```

## Cấu Hình

| Thuộc Tính | Giá Trị |
|----------|-------|
| **Topic Kafka** | `mysql.inventory.customers` |
| **CDC Format** | Debezium JSON Envelope |
| **Phiên Bản Spark** | 3.5.0 |
| **Phiên Bản Kafka Client** | 3.4.1 |
| **Phiên Bản Jedis** | 4.4.3 |
| **Kafka Bootstrap Servers** | `cdc-kafka:29092` |
| **Redis Host** | `cdc-redis` |
| **Redis Port** | `6379` |
| **Starting Offsets** | `latest` |
| **Trigger** | ProcessingTime 10 seconds |

## Tính Năng

- Tiêu thụ CDC events từ Kafka theo thời gian thực (Structured Streaming)
- Parse định dạng Debezium JSON Envelope
- Extract `after` state từ CDC payload
- Ghi dữ liệu vào Redis HSET (Hash Set)
- Real-time synchronization từ MySQL → Redis
- Batch processing với trigger mỗi 10 giây
- Error handling và connection management
- Exactly-once semantics

## Redis Data Structure

Dữ liệu được lưu trong Redis với cấu trúc:

```
Key: customer:{id}
Value: Hash Set
  - id: {customer_id}
  - name: {customer_name}
  - email: {customer_email}
```

**Ví dụ:**
```
customer:1 = {id: 1, name: SparkFix, email: fix@test.com}
customer:2 = {id: 2, name: TestUser, email: test@example.com}
```

## Quy Trình Git

Sau khi thực hiện các thay đổi trong module Spark-Redis, hãy commit riêng:

```bash
git add spark-redis/
git commit -m "Add Spark Structured Streaming CDC to Redis consumer"
git push
```

## Lưu Thay Đổi Trong Trình Soạn Thảo Nano

Nếu chỉnh sửa tệp bằng nano:

1. Nhấn: `Ctrl + O` (ghi ra)
2. Nhấn: `Enter` (xác nhận tên tệp)
3. Nhấn: `Ctrl + X` (thoát)

## Cấu Trúc Dự Án

```
spark-redis/
├── README.md                          # Tệp này
└── cdc_redis_consumer.scala           # Triển khai CDC consumer ghi vào Redis
```

## Khắc Phục Sự Cố

- **Giao diện Spark không truy cập được**: Đảm bảo cổng 8080 không bị chặn và các container đang chạy

- **Kết nối Kafka thất bại**: Xác minh hostname `cdc-kafka` và cổng `29092` đúng

- **Kết nối Redis thất bại**: Kiểm tra xem Redis container `cdc-redis` đang chạy
  ```bash
  docker ps | grep redis
  ```

- **Jedis library không tìm thấy**: Đảm bảo đã thêm `redis.clients:jedis:4.4.3` khi chạy spark-shell

- **Paste code gặp lỗi**: Luôn sử dụng `:paste` mode khi paste code vào spark-shell

- **Dữ liệu không xuất hiện trong Redis**: 
  - Kiểm tra logs Spark để tìm error
  - Xác minh rằng MySQL binlog được bật và Debezium connector chạy
  - Kiểm tra `startingOffsets` (nếu là "latest" sẽ bỏ qua dữ liệu cũ)

## Monitoring

Để theo dõi streaming job:

1. **Spark Master UI**: http://localhost:8080
2. **Spark Application UI**: http://localhost:4040 (khi job chạy)
3. **Redis CLI**: `docker exec -it cdc-redis redis-cli`

