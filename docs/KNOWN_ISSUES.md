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
| 6 | Monitoring Prometheus + Grafana | ✅ | Dashboard đầy đủ: real-time rates, Spark batch duration, Kafka lag, Grafana alerts |
| 7 | Benchmark E2E TPS | ⚠️ | Số liệu tổng thể OK, nhưng một số metrics phụ = 0 (xem mục 2) |
| 8 | Demo INSERT / UPDATE / DELETE CDC | ✅ | Script `demo.sh` và `docs/DEMO_SCRIPT.md` |

---

## Phần 2 — Vấn đề đo lường Benchmark

### ✅ Spark batch duration — đã fix (2026-05-17)

**Mô tả cũ (không còn đúng):** Metric là placeholder = 0.

**Thực tế hiện tại:** `StreamingQueryListener` đã được implement trong Scala job (`cdc_redis_consumer.scala`). Mỗi khi Spark hoàn thành một micro-batch có data, listener ghi `triggerExecution` ms vào Redis key `spark:batch_duration_ms`. Metrics exporter đọc key này và expose ra Prometheus. Grafana và benchmark script đều nhận đúng giá trị (test thực tế: ~6769ms khi pipeline đang chạy tải).

**Edge case còn lại:**
- Sau `stop.sh -v` + fresh start: metric = 0 cho đến khi batch đầu tiên có data xử lý xong (~vài giây).
- Chạy Python mode (`--python`): metric = 0 mãi vì Python job chưa implement listener này.

**Ảnh hưởng đến kết quả tổng thể:** Không còn ảnh hưởng khi dùng Scala mode.

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

### ✅ Panel real-time — đã có (Phase 3.1, 2026-05-07)

Row "Real-time Metrics" trong `cdc_fixed1.json` gồm: timeseries insert rate, Kafka rate, MongoDB write rate, lag, Spark batch duration ms, executor cores/memory. Metrics: `cdc_mysql_insert_rate`, `cdc_kafka_rate_total`, `cdc_mongo_write_rate`, `cdc_lag_total`, `cdc_kafka_consumer_lag`.

---

### ✅ Grafana datasource UID — đã tự động fix

`start.sh` bước 9 restart Grafana sau khi healthy để force reload provisioning. Sau `stop.sh -v` + `start.sh` dashboard load lại đúng UID. Nếu vẫn lỗi: `docker compose restart grafana`.

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
| 7 | Grafana alert khi lag > ngưỡng | ✅ `monitoring/grafana/provisioning/alerting/cdc_alerts.yml` — lag≥100(warn), lag≥500(critical), batch≥5000ms(warn) |
| 8 | ✅ Spark batch duration metric thật | Đã fix 2026-05-17: `StreamingQueryListener` → Redis → exporter (Scala mode) |
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

### ✅ Đã implement — hardware profiles + override flags (2026-05-06/07)

`.env.laptop` / `.env.server` / `.env.vm` tại root — `start.sh` auto-detect và copy sang `.env` trước khi compose up.

```bash
bash start.sh                        # auto-detect profile
bash start.sh --profile=server       # chỉ định rõ
bash start.sh --partitions=3 --spark-memory=4g  # override thông số
```

Biến môi trường trong `.env`:
```env
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=2g
KAFKA_HEAP_OPTS=-Xmx1g -Xms512m
KAFKA_NUM_PARTITIONS=1
```

**Ghi chú về Kafka partition:** Khi tăng partition >1, Spark xử lý song song được nhiều partition. Cần `SPARK_WORKER_COUNT >= KAFKA_NUM_PARTITIONS` để có hiệu quả thật.

---

## Lịch sử cập nhật

| Ngày | Thay đổi |
|---|---|
| 2026-04-19 | Hoàn thành pipeline cơ bản, benchmark E2E lần đầu |
| 2026-05-06 | Fix `start.sh` Windows compatibility, thêm rate metrics vào exporter, viết lại docs, pin requirements.txt, tạo .env.example, ghi nhận vấn đề thuật ngữ TPS |
| 2026-05-06 | Implement hardware profiles (.env.laptop/.env.server/.env.vm), start.sh auto-detect, override flags |
| 2026-05-07 | Phase 2B/2C: Redis counter fix, Grafana restart auto-patch, Spark executor metrics, StreamingQueryListener |
| 2026-05-07 | Phase 3: Grafana real-time panels, demo dashboard, benchmark history/compare, Grafana alert rules |
| 2026-05-16 | Phase 4: Spark checkpoint volume, Redis AOF persistence, fault tolerance demo |
| 2026-05-17 | Fix startup bug (partial containers skip `docker compose up -d`), thêm `cdc_kafka_consumer_lag` metric (delta-based) |
