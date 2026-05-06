# KNOWN ISSUES — CDC Pipeline

Tài liệu theo dõi các vấn đề đã biết, trạng thái xử lý, và những gì pipeline hiện tại chưa làm được.

---

## Ký hiệu

| Ký hiệu | Ý nghĩa |
|---|---|
| ✅ | Đã hoàn thành, hoạt động tốt |
| ⚠️ | Có hoạt động nhưng còn hạn chế / cần chú ý |
| ❌ | Chưa làm được / biết là sai nhưng chưa fix |
| 🔄 | Đang trong quá trình xử lý |

---

## Phần 1 — Tính năng chính

| # | Tính năng | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | CDC end-to-end: MySQL → Kafka → Spark → MongoDB | ✅ | Hoạt động ổn định |
| 2 | CDC end-to-end: MySQL → Kafka → Spark → Redis | ✅ | Hoạt động ổn định |
| 3 | Idempotent upsert (không duplicate khi event đến nhiều lần) | ✅ | MongoDB dùng `replaceOne + upsert`, Redis dùng `SET` |
| 4 | Spark checkpoint (không mất data khi restart) | ✅ | Checkpoint lưu tại `/tmp/spark-checkpoint/cdc-pipeline` trong container |
| 5 | Auto-fix Kafka Cluster ID conflict khi restart | ✅ | `start.sh` tự detect và xóa volume Kafka/Zookeeper |
| 6 | Monitoring Prometheus + Grafana | ⚠️ | Hoạt động, nhưng dashboard chưa đầy đủ (xem mục 3) |
| 7 | Benchmark E2E TPS | ⚠️ | Số liệu tổng thể OK, nhưng một số metrics phụ = 0 (xem mục 2) |
| 8 | Demo INSERT / UPDATE / DELETE CDC | ✅ | Script `demo.sh` và `docs/DEMO_SCRIPT.md` |

---

## Phần 2 — Vấn đề đo lường Benchmark

### ❌ Spark batch duration luôn = 0ms

**Mô tả:** Benchmark script (`run_benchmark_v4.py`) đọc metric `cdc_spark_batch_duration_ms` từ `metrics_exporter`, nhưng exporter **không có khả năng** lấy được thời gian xử lý từng batch của Spark Structured Streaming từ bên ngoài. Metric này hiện tại là placeholder = 0.

**Hệ quả:** Cột "Spark batch: avg 0ms, p95 0ms" trong output benchmark luôn là 0, không phản ánh thực tế.

**Cách fix đúng:** Cần expose Spark metrics ra ngoài thông qua Spark REST API (`/api/v1/applications/{id}/streaming/statistics`) hoặc dùng Spark `StreamingQueryListener` để ghi ra file rồi exporter đọc. Khá phức tạp, chưa làm.

**Ảnh hưởng đến kết quả tổng thể:** Thấp — E2E TPS vẫn đo đúng vì không phụ thuộc vào metric này.

---

### ⚠️ Thuật ngữ đang dùng không chính xác: TPS vs Row/s vs Event/s

**Mô tả:** Trong benchmark, 1 "TPS" = 1 `INSERT` MySQL = 1 row. Đây thực ra là **rows/s** hoặc **events/s**, không phải TPS (Transactions Per Second) theo định nghĩa chuẩn (1 transaction có thể gồm nhiều rows).

**Trong báo cáo nên viết rõ:** *"E2E throughput = X records/s (mỗi record tương ứng 1 CDC event, đo bằng số documents đến MongoDB chia tổng thời gian inject + drain)"*

---

### ⚠️ Python nhanh hơn Scala trong benchmark: kết quả sai do cách đo

**Mô tả:** Nếu benchmark cho thấy PySpark nhanh hơn Scala JAR, đó là artifact của:
- Trigger interval khác nhau: Scala = 5s, Python = 10s. Batch lớn hơn giảm overhead per-batch, cho số throughput cao hơn càng không hẳn.
- JVM JIT warmup: Scala lần đầu chậu chi phí compile, nếu benchmark bắt đầu ngay thì Scala bị thiệt.
- Benchmark đo theo MongoDB count poll 5s, không đo thời gian Spark xử lý thật.

**Thực tế:** Scala JVM nhanh hơn PySpark ở throughput cao vì không có Python bridge overhead. UDF Python đặc biệt chậm khi chạy từng row.

---

### ⚠️ Kafka rate và Mongo write rate = 0 lúc idle

**Mô tả:** Các metric `cdc_kafka_rate_total`, `cdc_mongo_write_rate`, `cdc_mysql_insert_rate` được tính bằng delta/5s giữa 2 lần poll. Khi không có load, delta = 0 → rate = 0. Đây là **đúng**, không phải bug.

**Khi benchmark đang chạy:** Rate sẽ có giá trị thực.

---

### ⚠️ `ram_gb = 0` trong kết quả benchmark

**Mô tả:** Script đọc RAM bằng `free -g | awk '/^Mem:/{print $2}'` — lệnh này chạy trong container Linux (metrics-exporter), nhưng container không nhận thấy RAM thật của host Windows.

**Hệ quả:** Trường `hardware.ram_gb` trong file JSON kết quả luôn = 0.

**Cách fix:** Dùng Docker API để lấy thông tin host, hoặc đọc từ `/proc/meminfo` trong container. Chưa ưu tiên fix vì chỉ ảnh hưởng đến metadata.

---

### ⚠️ Spark executor_cores và executor_memory_mb = 0

**Mô tả:** Tương tự ram_gb — metric Spark executor được lấy qua `cdc_spark_executor_cores` từ exporter, nhưng exporter hiện không query Spark REST API để lấy thông tin này.

---

## Phần 3 — Dashboard Grafana

### ❌ Không có panel real-time TPS

**Mô tả:** Dashboard hiện tại chỉ hiển thị số lượng records (MySQL count, Mongo count, Redis keys) và Kafka offset. Không có panel nào hiển thị TPS theo thời gian thực trong khi benchmark đang chạy.

**Metrics cần thiết đã có từ Phase 1 fix:** `cdc_mysql_insert_rate`, `cdc_kafka_rate_total`, `cdc_mongo_write_rate`, `cdc_lag_total` — đã được thêm vào exporter.

**Việc còn lại:** Tạo panel Grafana dùng các metrics trên. Dự kiến làm ở Phase 3.

---

### ⚠️ Grafana datasource UID đôi khi không khớp

**Mô tả:** Sau khi xóa volume và khởi động lại (`stop.sh -v` rồi `start.sh`), Grafana generate UID mới cho datasource Prometheus, nhưng dashboard JSON còn tham chiếu UID cũ → panel hiển thị "No data".

**Workaround:** `start.sh` có đoạn auto-patch UID, nhưng đoạn này hiện đang trống (chưa implement xong). Nếu bị lỗi, restart Grafana:
```bash
cd pipeline && docker compose restart grafana
```

---

## Phần 4 — Vấn đề tương thích Windows

### ✅ `start.sh` hiển thị `?` cho Debezium/Spark/Prometheus/Grafana

**Mô tả:** Đã fix — nguyên nhân là `python3` không tồn tại trong PATH của Git Bash trên Windows. Script đã được sửa để tự detect `python3` hoặc fallback sang `python`.

---

### ✅ Redis hiển thị trống thay vì số keys

**Mô tả:** Đã fix — bug `awk '{print $2}'` thay vì `awk '{print $1}'` trong lệnh đọc `redis-cli dbsize`.

---

### ⚠️ `curl` trong PowerShell không dùng được với script

**Mô tả:** Trong PowerShell, `curl` là alias của `Invoke-WebRequest`, không phải curl thật. Nếu chạy lệnh curl từ PowerShell, sẽ bị lỗi tham số.

**Giải pháp:** Luôn dùng **Git Bash** để chạy các script. Không dùng PowerShell cho dự án này.

---

## Phần 5 — Chưa làm / Việc cần làm

| # | Hạng mục | Ghi chú |
|---|---|---|
| 1 | Test với số Kafka partition > 1 | Hiện tại mặc định 1 partition. Benchmark `partition` mode hỗ trợ tăng lên 3 |
| 2 | Benchmark trên cloud VM | `docs/VM_SETUP.md` có hướng dẫn, nhưng chưa chạy thực tế |
| 3 | Schema registry / Avro serialization | Hiện tại dùng JSON plain, không có schema registry |
| 4 | TLS / authentication cho Kafka | Hiện tại plain text, phù hợp cho môi trường dev/test |
| 5 | DELETE event CDC — Redis chưa xóa key | Debezium capture đúng, Scala job có xử lý DELETE, nhưng Redis không xóa key |
| 6 | Multi-table CDC ngoài `customers` và `orders` | Hiện tại chỉ test 2 bảng |
| 7 | Grafana alert khi lag > ngưỡng | Chưa cấu hình |
| 8 | Spark batch duration metric thật | Kế hoạch: shared volume + `StreamingQueryListener` ghi JSON ra file |
| 9 | Benchmark với mix INSERT/UPDATE/DELETE thực tế | Hiện chỉ đo thuần INSERT customers |
| 10 | Benchmark với transaction nhiều rows (batch write) | Use case hẹp: hiện 1 "records/s" = 1 row |
| 11 | So sánh Scala vs Python bằng cách đo đúng | Cần đồng trigger interval: Python 10s → 5s trước |
| 12 | ✅ Pin version trong `requirements.txt` | Đã làm 2026-05-06 |
| 13 | ✅ Tạo `.env.example` | Đã làm 2026-05-06 |
| 14 | Đổi nhãn "TPS" → "records/s" trong benchmark output | Dễ, chỉ đổi label text trong `run_benchmark_v4.py` |
| 15 | Đồng trigger interval Python (10s → 5s, bằng Scala) | Dễ, 1 dòng code trong `cdc_pipeline.py` |
| 16 | Thêm benchmark mode `realistic` (60% INSERT / 30% UPDATE / 10% DELETE) | Trung bình, cần thêm logic vào `run_benchmark_v4.py` |
| 17 | Cơ chế scale tài nguyên theo phần cứng (xem Phần 6) | Cần thiết khi chạy trên hardware mạnh hơn |

---

## Phần 6 — Scale tài nguyên theo phần cứng

### ⚠️ Không có cơ chế cấu hình tài nguyên linh hoạt

**Hiện trạng:**
Docker Compose hiện tại **không có resource limits** và **không có biến môi trường** để tinh chỉnh Spark/Kafka theo hardware. Cụ thể:
- Không có `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`
- Số worker cứng = 3, không dễ thay đổi
- Kafka không có heap size setting
- Không có profile riêng cho laptop vs server vs VM

**Hệ quả:** Khi chuyển sang phần cứng mạnh hơn (nhiều core, RAM nhiều), phải tự tìm và sửa nhiều chỗ trong `docker-compose.yml` theo tay. Dễ sai và không reproducible.

---

**Kế hoạch fix — file `pipeline/.env` làm trung tâm cấu hình:**

Tạo file `pipeline/.env` (docker-compose tự đọc):
```env
# ── Spark Workers ─────────────────────────────────────
SPARK_WORKER_COUNT=3           # Số worker containers
SPARK_WORKER_CORES=4           # Core CPU mỗi worker
SPARK_WORKER_MEMORY=2g         # RAM mỗi worker

# ── Kafka ─────────────────────────────────────────────
KAFKA_HEAP_OPTS=-Xmx1g         # JVM heap Kafka broker
KAFKA_DEFAULT_PARTITIONS=1     # Partitions cho CDC topics

# ── Debezium Connect ──────────────────────────────────
CONNECT_HEAP=-Xmx512m
```

Rồi `docker-compose.yml` tham chiếu:
```yaml
environment:
  SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-4}
  SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-2g}
```

---

**Kế hoạch — profile theo môi trường:**

```
pipeline/
  .env                    # Active profile (copy từ một trong các file dưới)
  .env.laptop             # i5-11400H, 16GB RAM, Docker 8 cores
  .env.server             # 16+ cores, 32GB+, nhiều workers
  .env.vm                 # Cloud VM (DigitalOcean c-16...)
```

Khi switch phần cứng:
```bash
cp pipeline/.env.server pipeline/.env
bash stop.sh && bash start.sh
```

**Ghi chú thêm về Kafka partition:** Khi tăng Kafka partition (>1), Spark có thể xử lý song song nhiều partition cùng lúc. Muốn có hiệu quả thật sự cần đảm bảo `SPARK_WORKER_COUNT >= KAFKA_DEFAULT_PARTITIONS`.

---

## Lịch sử cập nhật

| Ngày | Thay đổi |
|---|---|
| 2026-04-19 | Hoàn thành pipeline cơ bản, benchmark E2E lần đầu |
| 2026-05-06 | Fix `start.sh` Windows compatibility, thêm rate metrics vào exporter, viết lại docs, pin requirements.txt, tạo .env.example, ghi nhận vấn đề thuật ngữ TPS, cơ chế scale tài nguyên |
