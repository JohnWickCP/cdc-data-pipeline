# Benchmark Results — CDC Real-time Data Pipeline

Kết quả đo lường hiệu năng thực tế của CDC pipeline trên phần cứng laptop.

---

## Môi trường thử nghiệm

| Mục | Thông số |
|---|---|
| **CPU** | 11th Gen Intel Core i5-11400H @ 2.70GHz (6 cores / 12 threads) |
| **RAM** | 16 GB |
| **OS** | Windows 10 Pro + Docker Desktop (WSL2 backend) |
| **Docker** | v28.0.1 / Docker Compose v2.33.1 |
| **Spark** | 3.5.0 — 3 Workers × 4 cores × 2GB RAM = **12 cores / 6GB tổng** |
| **Kafka** | Confluent 7.5.0 — 1 partition (default) |
| **Spark job** | Scala JAR (`CdcRedisConsumer`) — trigger interval 5s |
| **Pipeline** | MySQL → Debezium → Kafka → Spark Streaming → MongoDB + Redis |

---

## Kết quả Benchmark — Quick Mode (2026-05-16)

**Phương pháp đo:** E2E throughput thật = records đến MongoDB ÷ (thời gian inject + thời gian drain hết lag).
Không tính theo inject rate — tính theo số record thực sự đã được pipeline xử lý hoàn chỉnh.

### Throughput theo mức tải

| Mức inject | Inject rate thực | E2E Throughput | Lag cuối | Thời gian drain | Spark batch avg | Spark batch p95 |
|---|---|---|---|---|---|---|
| 100 rec/s | 99.8 rec/s | **92.7 rec/s** | 0 | 1.5s | 672 ms | 1,248 ms |
| 200 rec/s | 199.5 rec/s | **149.9 rec/s** | 0 | 6.6s | 669 ms | 1,088 ms |
| 500 rec/s | 497.1 rec/s | **404.3 rec/s** | 0 | 4.6s | 770 ms | 1,362 ms |

### Sustained Throughput

| Metric | Giá trị |
|---|---|
| **Tải kiểm tra** | 323 rec/s × 30s |
| **E2E Throughput** | **273.3 rec/s** |
| **Records vào MongoDB** | 9,690 |
| **Kafka Lag cuối** | 0 |
| **Spark p50** | 923 ms |
| **Spark p95** | 2,363 ms |
| **Bottleneck** | Không có |

### Tóm tắt

| Metric | Giá trị |
|---|---|
| **Max E2E Throughput** | **404.3 records/s** |
| **Sustained Throughput** | **273.3 records/s** |
| **Kafka partitions** | 1 |
| **Spark engine** | Scala (JAR) |
| **Kafka lag** | 0 tại mọi mức test |

---

## So sánh Scala vs Python

| Metric | Scala JAR | PySpark | Tỷ lệ |
|---|---|---|---|
| **Max E2E** | 404 rec/s | ~83 rec/s | **~4.9× nhanh hơn** |
| **Sustained** | 273 rec/s | ~55 rec/s | **~5× nhanh hơn** |
| **Spark p50** | ~670–923 ms | — | — |
| **Spark p95** | ~1,088–2,363 ms | — | — |

> Python benchmark kém tin cậy hơn do DELETE events tích lũy trong Kafka làm sai drain condition.
> Scala là engine chính thức của project.

---

## Lịch sử benchmark

| Ngày | Engine | Mode | Max rec/s | Sustained rec/s | Spark p50 | Spark p95 |
|---|---|---|---|---|---|---|
| 2026-05-07 | Scala | quick | 370.0 | 249.7 | 1,002 ms | 2,493 ms |
| 2026-05-07 | Python | quick | 82.5 | — | — | — |
| **2026-05-16** | **Scala** | **quick** | **404.3** | **273.3** | **923 ms** | **2,363 ms** |

---

## Trạng thái dữ liệu thực tế (tại thời điểm benchmark)

| Bảng | MySQL | MongoDB | Redis | Trạng thái |
|---|---|---|---|---|
| customers | 21,950 | 21,950 | 21,950 | ✅ In sync |
| orders | 19,648 | 19,648 | 19,648 | ✅ In sync |
| Kafka customers offset | — | — | — | 99,173 events |
| Kafka orders offset | — | — | — | 19,648 events |

---

## Metrics từ Prometheus (snapshot)

| Metric | Giá trị |
|---|---|
| `cdc_pipeline_up` | 1 (running) |
| `cdc_mysql_mongo_in_sync` | 1 (in sync) |
| `cdc_lag_total` | 0 (no lag) |
| `cdc_spark_executor_cores` | 12 |
| `cdc_spark_executor_memory_mb` | 3,072 MB |
| `cdc_spark_batch_duration_ms` | ~2,503 ms (post-test) |
| `cdc_benchmark_throughput_e2e` | 404.3 |
| `cdc_benchmark_latency_p95` | 2.363 s |

---

## Smoke Test

**43/43 PASS** — tất cả test cases đều pass, bao gồm:
- 13 containers running & healthy
- 9 port connectivity checks
- Debezium connector + task: RUNNING
- Kafka topics: `inventory.inventory.customers`, `inventory.inventory.orders`
- Spark: 1 active app, 3 workers alive
- MySQL ↔ MongoDB in sync
- E2E insert test: record xuất hiện trong MongoDB sau **3 giây**
- 6 metrics present trong Prometheus
- 3 hardware profiles (.env.laptop/server/vm)

---

## Ghi chú về phương pháp đo

- **E2E records/s** ≠ inject rate. E2E = số records thực sự đến MongoDB / tổng thời gian.
- **Drain time** = thời gian pipeline xử lý hết số records còn lag sau khi inject xong.
- **Spark batch duration** = thời gian Spark xử lý 1 micro-batch (đo qua StreamingQueryListener → Redis → metrics exporter).
- Benchmark dùng Kafka partition=1 (default). Tăng partition có thể tăng throughput nhưng chưa test chính thức.
- Xem thêm: [docs/CLARIFICATIONS.md](CLARIFICATIONS.md)
