# Troubleshooting — Các Lỗi Gặp Phải Khi Setup CDC Pipeline

Tài liệu này liệt kê các lỗi thực tế gặp phải trong quá trình setup pipeline, lý do xảy ra, và cách fix.

---

## Lỗi 1: Connector trả về 404 — `No status found for connector mysql-connector`

**Lệnh gặp lỗi:**
```bash
curl -s http://localhost:8083/connectors/mysql-connector/status
```

**Output lỗi:**
```json
{
    "error_code": 404,
    "message": "No status found for connector mysql-connector"
}
```

**Nguyên nhân:**
Connector chưa được đăng ký, hoặc đã bị mất sau khi restart container. Debezium lưu connector config trong Kafka topic `connect_configs` — nếu volume bị xóa hoặc container được recreate với `--volumes`, toàn bộ config sẽ mất.

**Cách fix:**
```bash
cd ~/cdc-pipeline/demo
bash register-connector.sh

# Verify connector đã đăng ký
curl http://localhost:8083/connectors
# Kết quả mong đợi: ["mysql-connector"]
```

---

## Lỗi 2: Spark `foreachBatch` — `ambiguous reference to overloaded definition`

**Output lỗi:**
```
error: ambiguous reference to overloaded definition,
both method foreachBatch in class DataStreamWriter of type
(function: org.apache.spark.api.java.function.VoidFunction2[...])
and method foreachBatch in class DataStreamWriter of type
(function: (Dataset[Row], Long) => Unit)
match argument types ((Dataset[Row], Any) => Unit)
      .foreachBatch { (batchDF, batchId) =>
```

**Nguyên nhân:**
Scala compiler không thể xác định được method nào trong 2 overload của `foreachBatch` cần dùng vì `batchId` không có type annotation rõ ràng — compiler suy ra là `Any` thay vì `Long`, dẫn đến ambiguous match.

**Cách fix:**
Tách `foreachBatch` thành một method riêng với type signature rõ ràng, sau đó truyền method reference vào:

```scala
// Thay vì viết inline:
.foreachBatch { (batchDF, batchId) => ... }

// Khai báo method riêng với type rõ ràng:
def processBatch(batchDF: Dataset[Row], batchId: Long): Unit = {
  ...
}

// Truyền vào bằng method reference:
.foreachBatch(processBatch _)
```

---

## Lỗi 3: Spark không nhận được resource — `Initial job has not accepted any resources`

**Output lỗi:**
```
WARN TaskSchedulerImpl: Initial job has not accepted any resources;
check your cluster UI to ensure that workers are registered
and have sufficient resources
```

**Nguyên nhân:**
Khi chạy `spark-shell` bên trong container `cdc-spark-master` với `--master spark://cdc-spark-master:7077`, Spark sẽ dùng cluster mode — worker cần kết nối ngược lại driver qua port động (37063, 41703...). Tuy nhiên, port này chỉ open bên trong container, worker ở container khác không thể reach được driver, dẫn đến executor bị kill liên tục.

**Cách fix:**
Dùng `--master local[2]` thay vì cluster mode khi chạy spark-shell bên trong container. Với demo này, local mode đủ dùng và tránh được vấn đề network giữa các container:

```bash
# Thay vì:
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master spark://cdc-spark-master:7077 ...

# Dùng:
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] ...
```

---

## Lỗi 4: File không tồn tại trong container — `File does not exist`

**Output lỗi:**
```
warning: File `/opt/spark/work/cdc_redis_consumer.scala' does not exist.
```

**Nguyên nhân:**
Thư mục `/opt/spark/work` không tồn tại trong image `apache/spark:3.5.0`. Thư mục mặc định của Spark container là `/opt/spark/work-dir`.

**Cách fix:**
```bash
# Copy file vào đúng thư mục
docker cp ~/cdc-pipeline/spark-redis/cdc_redis_consumer.scala \
  cdc-spark-master:/opt/spark/work-dir/

# Chạy với đường dẫn đúng
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala
```

---

## Lỗi 5: Spark Batch luôn trả về 0 records — `=== Batch N (0 records) ===`

**Triệu chứng:**
Spark chạy bình thường, không có lỗi, nhưng mọi batch đều báo 0 records dù Kafka đã có message.

**Nguyên nhân:**
Debezium gửi message với cấu trúc 2 lớp:
```json
{
  "schema": { ... },
  "payload": {
    "op": "c",
    "before": null,
    "after": { "id": 1, ... }
  }
}
```

Code ban đầu parse trực tiếp `from_json(json_str, payloadSchema)` — tức là coi toàn bộ JSON là payload. Vì schema không khớp (thiếu lớp `payload` wrapper bên ngoài), Spark parse ra `null` toàn bộ, filter lọc sạch hết record.

**Cách fix:**
Định nghĩa schema wrapper bên ngoài bao gồm field `payload`, rồi truy cập `root.payload`:

```scala
val debeziumRootSchema: StructType = new StructType()
  .add("payload", payloadInnerSchema, nullable = true)

// Khi parse:
.withColumn("root",    from_json($"json_str", debeziumRootSchema))
.withColumn("payload", $"root.payload")
.select(
  $"payload.op".as("op"),
  $"payload.before".as("before"),
  $"payload.after".as("after")
)
```

---

## Lỗi 6: Spark đọc lại từ đầu sau khi xóa checkpoint — Records bị duplicate

**Triệu chứng:**
Sau khi xóa `/tmp/spark-checkpoint` và chạy lại, Spark đọc lại toàn bộ message từ `earliest`, ghi duplicate vào MongoDB và Redis.

**Nguyên nhân:**
Checkpoint lưu offset đã xử lý. Khi xóa checkpoint, Spark mất thông tin offset và bắt đầu lại từ `startingOffsets = earliest`.

**Cách fix:**
Chỉ xóa checkpoint khi cần reset có chủ ý (ví dụ: thay đổi schema). Khi restart bình thường, giữ nguyên checkpoint:

```bash
# Restart Spark mà KHÔNG xóa checkpoint → không duplicate
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala

# Chỉ xóa checkpoint khi muốn reset hoàn toàn
docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint
```

Ngoài ra, việc dùng `replaceOne + upsert=true` (MongoDB) và `SET` (Redis) đảm bảo dù có đọc lại thì data cuối cùng vẫn đúng — không tạo record thừa.

---

## Bảng Tóm Tắt

| # | Lỗi | Nguyên nhân chính | Fix nhanh |
|---|---|---|---|
| 1 | Connector 404 | Chưa đăng ký hoặc mất sau restart | Chạy lại `register-connector.sh` |
| 2 | `foreachBatch` ambiguous | `batchId` không có type `Long` | Tách thành method riêng với type rõ ràng |
| 3 | No resources accepted | Worker không reach được driver port trong cluster mode | Dùng `--master local[2]` |
| 4 | File does not exist | Sai đường dẫn thư mục trong container | Dùng `/opt/spark/work-dir/` |
| 5 | Batch 0 records | Schema thiếu lớp wrapper `payload` của Debezium | Thêm `debeziumRootSchema` bọc ngoài |
| 6 | Duplicate sau restart | Xóa checkpoint khiến Spark đọc lại từ đầu | Giữ checkpoint khi restart bình thường |