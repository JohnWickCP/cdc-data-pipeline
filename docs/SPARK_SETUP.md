# Spark Structured Streaming — Hướng dẫn Setup & Chạy

Hướng dẫn này mô tả toàn bộ quá trình setup và chạy Spark Structured Streaming job để consume CDC events từ Kafka, sau đó ghi vào Redis và MongoDB.

---

## Yêu cầu trước khi bắt đầu

Trước khi chạy Spark job, đảm bảo các service sau đã sẵn sàng:

- `cdc-kafka` đang chạy và có topic `mysql.inventory.customers`
- `cdc-mongodb` đang chạy tại port `27017`
- `cdc-redis` đang chạy tại port `6379`
- `cdc-spark-master` và `cdc-spark-worker-*` đang Up

Kiểm tra nhanh:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|mongo|redis|spark"
```

---

## Bước 1: Build Spark Image với Dependencies

Image Spark cần được build với đầy đủ JAR dependencies trước khi chạy job. JAR cần thiết gồm: Kafka connector, MongoDB driver, và Jedis (Redis client).

```bash
cd ~/cdc-pipeline/spark
docker build -t cdc-spark:3.5.0 .
```

Sau khi build xong, verify JAR đã có trong image:
```bash
docker exec cdc-spark-master ls /opt/spark/jars/ | grep -E "kafka|mongo|jedis"
```

Kết quả mong đợi:
```
jedis-5.1.0.jar
kafka-clients-3.4.0.jar
mongodb-driver-core-4.11.0.jar
mongodb-driver-sync-4.11.0.jar
mongo-spark-connector_2.12-10.3.0.jar
spark-sql-kafka-0-10_2.12-3.5.0.jar
spark-token-provider-kafka-0-10_2.12-3.5.0.jar
```

---

## Bước 2: Restart Spark Containers

Sau khi build image mới, restart các Spark containers để áp dụng thay đổi:

```bash
cd ~/cdc-pipeline/pipeline
docker compose up -d --no-deps --force-recreate spark-master spark-worker-1 spark-worker-2
```

Verify 3 containers đang Up:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep spark
```

---

## Bước 3: Copy Scala File vào Container

```bash
docker cp ~/cdc-pipeline/spark-redis/cdc_redis_consumer.scala cdc-spark-master:/opt/spark/work-dir/
```

Verify file đã có:
```bash
docker exec cdc-spark-master ls -la /opt/spark/work-dir/
```

---

## Bước 4: Chạy Spark Job

```bash
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala
```

> **Lưu ý**: Dùng `--master local[2]` thay vì `spark://cdc-spark-master:7077` vì khi chạy spark-shell bên trong container, worker không thể kết nối ngược lại driver port từ bên ngoài.

Spark sẽ khởi động và bắt đầu xử lý. Output mong đợi sau 5–10 giây:

```
=== Batch 0 (5 records) ===
[MongoDB] UPSERT id=1 name=Alice email=alice@example.com
[Redis] SET customer:1
[MongoDB] UPSERT id=2 name=Bob email=bob@example.com
[Redis] SET customer:2
...
```

---

## Bước 5: Test End-to-End

Trong terminal mới, insert dữ liệu vào MySQL:

```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
INSERT INTO customers (name, email) VALUES ('NewUser', 'new@example.com');
"
```

Chờ 5–10 giây, quay lại terminal Spark sẽ thấy batch mới xử lý record vừa insert.

Verify dữ liệu đã vào Redis và MongoDB:

```bash
# Redis
docker exec cdc-redis redis-cli GET customer:1

# MongoDB
docker exec cdc-mongodb mongosh --eval \
  "db.getSiblingDB('inventory').customers.find().pretty()" --quiet
```

---

## Bước 6: Test Idempotent Upsert

Chạy UPDATE trên cùng 1 record nhiều lần — MongoDB và Redis chỉ giữ giá trị mới nhất, không tạo duplicate:

```bash
docker exec cdc-mysql mysql -uroot -proot -e \
  "USE inventory; UPDATE customers SET email='updated@test.com' WHERE id=1;"

# Verify: đếm documents trong MongoDB không tăng
docker exec cdc-mongodb mongosh --eval \
  "db.getSiblingDB('inventory').customers.countDocuments()" --quiet

# Verify: Redis trả về email mới nhất
docker exec cdc-redis redis-cli GET customer:1
```

---

## Bước 7: Test Checkpoint (Không Duplicate Sau Restart)

```bash
# 1. Dừng Spark (Ctrl+C trong terminal đang chạy)

# 2. Insert thêm data trong lúc Spark không chạy
docker exec cdc-mysql mysql -uroot -proot -e \
  "USE inventory; INSERT INTO customers (name, email) VALUES ('OfflineUser', 'offline@test.com');"

# 3. Restart Spark (KHÔNG xóa checkpoint)
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala

# 4. Spark sẽ tiếp tục từ offset đã lưu, xử lý đúng record bị bỏ lỡ
# Verify: record 'OfflineUser' xuất hiện trong MongoDB
docker exec cdc-mongodb mongosh --eval \
  "db.getSiblingDB('inventory').customers.find({name:'OfflineUser'}).pretty()" --quiet
```

---

## Cấu trúc Spark Job

```
Kafka (mysql.inventory.customers)
    ↓  readStream (format=kafka)
Parse JSON (Debezium wrapper: root.payload.op/before/after)
    ↓  from_json với debeziumRootSchema
Extract record (after nếu insert/update, before nếu delete)
    ↓  foreachBatch mỗi 5 giây
    ├── MongoDB: replaceOne + upsert=true  (idempotent)
    └── Redis:   SET key value             (idempotent overwrite)
```

---

## Dừng Spark Job

Nhấn `Ctrl+C` trong terminal đang chạy spark-shell. Checkpoint sẽ được giữ lại tại `/tmp/spark-checkpoint/cdc-customers/` bên trong container.

Để xóa checkpoint (reset về trạng thái ban đầu, đọc lại từ đầu):
```bash
docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint
```

---

## Web UI

| Service | URL | Mô tả |
|---|---|---|
| Spark Master | http://localhost:8080 | Xem workers, jobs đang chạy |
| Spark App UI | http://localhost:4040 | Chi tiết streaming queries, stages |