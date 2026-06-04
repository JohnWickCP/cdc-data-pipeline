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

## 4. `cdc_spark_batch_duration_ms` — đã hoạt động đúng (cập nhật 2026-05-17)

### Cơ chế hiện tại

Metric được đo qua chuỗi: **Scala `StreamingQueryListener` → Redis → metrics exporter → Prometheus**.

Cụ thể: `StreamingQueryListener.onQueryProgress` trong `cdc_redis_consumer.scala` đọc `triggerExecution` từ `event.progress.durationMs` sau mỗi micro-batch có data, rồi ghi vào Redis key `spark:batch_duration_ms`. Exporter đọc key này mỗi 5 giây.

### Khi nào metric = 0

- Ngay sau `stop.sh -v` + fresh start: key chưa được ghi lần nào → = 0 cho đến batch đầu tiên có data (vài giây).
- Khi chạy Python mode (`--python`): Python job chưa implement listener này → = 0 mãi.

### Giá trị thực tế

Khi pipeline đang xử lý tải bình thường (Scala mode): ~1000–7000ms tùy throughput.

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
- **Throughput dưới concurrent load**: benchmark inject single-threaded

*Đã fix so với phiên bản cũ:*
- ~~Spark batch duration = 0~~ → ✅ `StreamingQueryListener` → Redis → exporter (Scala mode, từ 2026-05-07)
- ~~RAM/CPU Spark executors = 0~~ → ✅ `collect_spark()` query Spark Master REST API (từ 2026-05-07)

### Lưu ý khi so sánh với benchmark công ty khác

Các hệ thống production thường benchmark với:
- Network latency thật (cross-datacenter)
- Concurrent producers/consumers
- Replication factor > 1

Số 400 records/s trên laptop Docker là **throughput thật của setup localhost**, không phải giới hạn trên của công nghệ.

---

## 9. "Drain" là gì trong benchmark?

### Định nghĩa

**Drain** = giai đoạn chờ sau khi dừng inject, để pipeline xử lý hết phần còn tồn đọng.

```
[inject phase]  INSERT 500 records vào MySQL  (t = 0 → 5s)
[drain phase]   Spark đang xử lý backlog       (t = 5s → ~10s)
                → MongoDB nhận đủ 500 documents
[done]          E2E time = 10s → E2E rate = 50 rec/s
```

### Tại sao cần drain?

Spark Structured Streaming dùng **micro-batch với trigger 5s**. Khi bạn dừng inject ở giây thứ 5, Spark vẫn đang xử lý batch hiện tại. Nếu đo ngay, MongoDB chưa nhận hết → E2E rate bị undercount.

### Drain condition trong code

```python
# Đúng (delta-based):
target_mongo = before_mongo + mysql_delta
while mongo_count < target_mongo:
    time.sleep(0.5)

# Sai (absolute, đã fix):
# while mongo_count < after_mysql:  ← bị lừa bởi data cũ từ run trước
```

---

## 10. "Latency" trong CDC pipeline — đo gì và bao nhiêu?

### Latency per-record (không đo được trực tiếp)

Benchmark này **không đo latency từng record** vì không có timestamp gắn vào mỗi message.

Latency thực tế ≈ **Spark trigger interval** = 5s (worst case):
- Record INSERT vào MySQL ngay trước trigger → phải chờ 5s đến batch tiếp theo
- Record INSERT ngay sau trigger → chờ ~0s (xử lý trong batch hiện tại)
- Trung bình: ~2.5s (khi không có backlog)

### Spark batch duration (đo được sau fix 2C.1)

`spark:batch_duration_ms` trong Redis = thời gian Spark xử lý 1 micro-batch:
- ~350–1300ms: Spark đọc Kafka → transform → ghi MongoDB + Redis
- Nhỏ hơn trigger interval (5000ms) → tốt (pipeline không bị trễ)
- Bằng hoặc lớn hơn 5000ms → cảnh báo (Grafana alert kích hoạt)

### Kafka lag (đo được — hai loại)

**`cdc_lag_total`** = `mysql_customers_count - mongo_customers_count`
- Đo sync gap giữa MySQL và MongoDB (không phải Kafka consumer lag)
- = 0 khi idle, tăng khi MySQL có records chưa đến MongoDB
- Luôn = 0 ở trạng thái nghỉ (đây là đúng, không phải bug)

**`cdc_kafka_consumer_lag`** (thêm từ 2026-05-17) = delta Kafka events - delta MongoDB writes trong mỗi polling window 5s
- Đo backpressure real-time: Kafka nhận nhanh hơn Spark xử lý
- = 0 khi idle, spike khi có load cao, về 0 khi Spark bắt kịp
- Đây là proxy (Spark dùng checkpoint, không có consumer group chuẩn)

---

## 11. Tại sao con số benchmark trông ấn tượng — và test chuẩn hơn sẽ như thế nào?

### Tại sao 370 rec/s trên laptop trông cao?

Setup hiện tại được **tối ưu hóa không chủ ý**:

| Yếu tố | Setup này | Production thật |
|---|---|---|
| Network | Localhost Docker bridge ≈ 0ms | Cross-datacenter 1–50ms |
| MySQL → Kafka | In-container loopback | Network TCP thật |
| Replication | 0 (single broker, single node) | Kafka RF=3, MongoDB replica set |
| Concurrent load | Single-threaded inject | Multi-producer, multi-consumer |
| Durability | fsync tắt trong dev | Bật, gây I/O wait |

**Kết quả**: Mọi latency đều bị loại bỏ, chỉ còn Spark processing time. Số 370 rec/s là **real nhưng optimistic** — đây là ceiling của single-node localhost setup, không phải throughput kỳ vọng của distributed cluster.

### Điểm thực sự ấn tượng

Con số không phải thứ nên highlight. Thứ đáng nói là:
- **Kiến trúc scale ngang được**: thêm Kafka partition → thêm Spark worker → throughput tăng linear
- **Consistency**: Spark exactly-once semantics, idempotent MongoDB upsert
- **Observability**: metrics từ mọi stage (MySQL → Kafka → Spark → MongoDB → Redis)
- **Fault tolerance**: nếu Spark crash, checkpoint tự resume từ offset cuối

### Cách test chuẩn hơn trong tương lai

#### 1. Đo latency per-record (hiện tại không có)
```sql
-- Thêm cột timestamp vào MySQL
ALTER TABLE customers ADD COLUMN created_at DATETIME(3) DEFAULT NOW(3);
-- Debezium truyền created_at → Spark → MongoDB
-- Đo: MongoDB.insert_time - MySQL.created_at = true E2E latency
```
Expected: 2–6s (do Spark trigger 5s)

#### 2. Multi-producer concurrent load
```python
# Thay vì 1 thread inject:
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as ex:
    ex.map(inject_batch, [rate//4]*4)
# → Stress test connection pool, transaction isolation
```

#### 3. Kafka replication + fault test
```bash
# Tăng replication factor lên 3 (cần 3 Kafka brokers)
KAFKA_NUM_PARTITIONS=3 bash start.sh
# Kill 1 broker → đo recovery time
```

#### 4. Sustained load test (8h+)
Hiện tại benchmark chạy tối đa vài phút. Production cần:
- Chạy 8 giờ liên tục, đo throughput drift
- Monitor memory leak trong Spark
- Xem Kafka offset có tích lũy dần không

#### 5. So sánh Kafka partition scaling
Benchmark mode `partition` đã có — chạy với 1, 2, 4, 8 partitions:
```bash
bash scripts/run_bench.sh partition
python benchmark/compare_runs.py --mode partition
```
Expected: throughput tăng ~linear đến khi bottleneck chuyển sang MongoDB write.
