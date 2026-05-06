# CLARIFICATIONS.md — Các khái niệm dễ nhầm trong CDC Pipeline

Tài liệu này giải thích các thuật ngữ và con số trong project mà có thể gây hiểu lầm nếu không biết bối cảnh.

---

## 1. TPS vs records/s vs events/s — cái nào đúng?

### Định nghĩa

| Thuật ngữ | Ý nghĩa chuẩn | Dùng ở đâu |
|---|---|---|
| **TPS** (Transactions/s) | 1 transaction = 1 `BEGIN...COMMIT`, có thể chứa nhiều row | Đúng cho OLTP database benchmark |
| **records/s** | Số rows/documents xử lý mỗi giây | Đúng cho pipeline data throughput |
| **events/s** | Số CDC events mỗi giây — mỗi row-level change = 1 event | Đúng cho Kafka / Debezium throughput |

### Vấn đề với "TPS" trong context này

Benchmark này inject từng row độc lập, đo số documents đến MongoDB — không đo transactions. Dùng "TPS" là sai label.

Ví dụ minh họa sự khác biệt:

```sql
-- Đây là 1 TPS nhưng 1000 records:
BEGIN;
INSERT INTO customers VALUES (...), (...), ...;  -- 1000 rows
COMMIT;

-- Đây là 1000 TPS và 1000 records (benchmark của chúng ta):
INSERT INTO customers VALUES (...);  -- ×1000 lần
```

### Trong pipeline này

```
MySQL INSERT 1 row
  → Debezium: 1 CDC event (op = c / insert)
  → Kafka:    1 message
  → Spark:    1 record processed
  → MongoDB:  1 document upserted
```

**Quick mode** (chỉ INSERT): `records/s = events/s = rows/s` — ba cái bằng nhau.

**Realistic mode** (INSERT + UPDATE + DELETE):

| Operation | Kafka events | MongoDB documents mới |
|---|---|---|
| INSERT | 1 event | +1 document |
| UPDATE | 1 event | 0 (sửa doc cũ) |
| DELETE | 1 event | 0 (xóa doc) |

→ Kafka rate nên dùng **events/s** (đếm mọi loại operation).
→ E2E throughput nên dùng **records/s** (đếm tổng thay đổi đến MongoDB).

### Kết luận

Benchmark hiện tại dùng đúng:
- Inject rate → `records/s` (rows mới insert vào MySQL) ✅
- Kafka rate → `events/s` ✅
- E2E rate → `records/s` (documents đến MongoDB) ✅
- "TPS" → đã đổi sang `records/s` ✅

---

## 2. "E2E records/s" — đo cái gì chính xác?

### Định nghĩa trong benchmark này

```
E2E records/s = mongo_delta / (inject_time + drain_time)
```

- `mongo_delta`: số documents tăng thêm trong MongoDB từ đầu đến cuối
- `inject_time`: thời gian chạy INSERT vào MySQL
- `drain_time`: thời gian chờ MongoDB nhận hết (tối đa `max_drain_s`)

### Tại sao con số có vẻ cao?

Trên laptop (i5-11400H, Docker localhost):
- MySQL → Kafka → Spark → MongoDB đều chạy trên **cùng một máy**
- Latency giữa các service ≈ 0ms (không có network thật)
- Spark trigger 5s → drain sau inject rất nhanh

Kết quả ~400 records/s trên laptop là **trung thực cho setup này**, nhưng không thể so trực tiếp với môi trường distributed thật (nơi network latency chiếm phần lớn).

---

## 3. `customers:total` trong Redis — đếm gì?

### Hiểu sai thường gặp

Nghe tên `customers:total` thì nghĩ là **số customers hiện tại trong hệ thống**.

### Thực tế (trước khi fix bug 2B.1)

Đây là counter tăng bất kỳ lúc nào có event đến — kể cả UPDATE. Nên nó thực ra là **số lần có sự kiện liên quan đến customers**, không phải số khách hàng.

```
INSERT customer_1  → counter: 1  ✅
INSERT customer_2  → counter: 2  ✅
UPDATE customer_1  → counter: 3  ← SAI (tăng khi update)
DELETE customer_2  → counter: 3  ← SAI (không giảm khi xóa)
```

Kết quả: sau test có 1 customer, Redis báo `customers:total = 3`.

### Sau fix (bug 2B.1)

Counter chỉ tăng khi `op = c` (INSERT) hoặc `op = r` (snapshot read), giảm khi `op = d` (DELETE).

---

## 4. `cdc_spark_batch_duration_ms` luôn = 0

### Lý do

Metric này là **placeholder** — không đo được từ bên ngoài Spark mà không có Spark REST API hoặc `StreamingQueryListener` bên trong job.

Metrics exporter là một Python process riêng ngoài Spark, không có cách nào đọc batch duration thật qua HTTP.

### Hậu quả

Benchmark in ra `Spark batch: avg 0ms, p95 0ms` — con số này vô nghĩa, không phải Spark đang xử lý 0ms/batch.

Để fix đúng cần implement `StreamingQueryListener` trong Scala job và expose ra qua một endpoint (xem TASKS.md 2C.1).

---

## 5. `ram_gb = 0` trong benchmark JSON

### Nguyên nhân kỹ thuật

Benchmark chạy bên trong container Alpine Linux (busybox). Lệnh `free -g` **không được busybox hỗ trợ** — busybox `free` chỉ nhận `-b`, `-k`, `-m`. Khi flag không hợp lệ:

```bash
free -g | awk '/^Mem:/{print $2}'
# → awk nhận input rỗng → không in gì
```

Python nhận chuỗi rỗng:
```python
"" or "0"  # → "0"
int("0")   # → 0  ← bug
```

### Fix

Dùng `free -m` (MB, busybox hỗ trợ) rồi chia trong Python:
```python
int(run("free -m | awk '/^Mem:/{print $2}'") or "0") / 1024
# → 31.2 GB thay vì 0
```

---

## 6. Python trigger 10s vs Scala trigger 5s

### Vấn đề

`jobs/scala/cdc_redis_consumer.scala`: `.trigger(Trigger.ProcessingTime("5 seconds"))`
`jobs/python/cdc_pipeline.py`: `.trigger(processingTime="5 seconds")`

**Cả hai đều là 5s** — không có sự khác biệt. Ghi chú cũ trong TASKS.md về "Python 10s" là không còn chính xác.

### Hệ quả khi so sánh benchmark Scala vs Python

Sự khác biệt giữa hai mode không phải do trigger interval mà do:
- Scala JAR: compiled, chạy nhanh hơn
- PySpark: Python serialization overhead trong `foreachBatch`
- Cần test riêng để có số so sánh trung thực

---

## 7. Spark "aliveworkers" — là int, không phải list

### Context

Khi kiểm tra Spark Master REST API (`/json/`), trường `aliveworkers` trả về một **integer** (số worker đang active), không phải array.

Smoke test cũ dùng `len()` trên kết quả → crash. Fix: đọc trực tiếp as int.

```python
# SAI:
alive = len(data["aliveworkers"])

# ĐÚNG:
alive = data["aliveworkers"]
```

---

## 8. "Benchmark" trong project này đo gì — và không đo gì

### Đo được

- **E2E throughput** (records/s): từ MySQL insert đến MongoDB upsert, đo tổng thời gian
- **Inject rate**: MySQL write speed (phụ thuộc connection overhead, batch size)
- **Kafka lag**: còn bao nhiêu messages chưa được consume
- **Drain time**: Spark mất bao lâu xử lý hết backlog sau khi dừng inject

### Không đo được / không chính xác

- **Latency per-record**: chỉ đo aggregate, không có timestamp per-message
- **Spark batch duration**: luôn = 0 (placeholder, xem mục 4)
- **RAM/CPU của Spark executors**: lấy từ metric không tồn tại, luôn = 0
- **Throughput dưới concurrent load**: benchmark inject single-threaded

### Lưu ý khi so sánh với benchmark công ty khác

Các hệ thống production thường benchmark với:
- Network latency thật (cross-datacenter)
- Concurrent producers/consumers
- Replication factor > 1

Số 400 records/s trên laptop Docker là **throughput thật của setup localhost**, không phải giới hạn trên của công nghệ.
