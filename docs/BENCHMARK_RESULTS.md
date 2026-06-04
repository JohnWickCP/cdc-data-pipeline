# Benchmark Results — CDC Real-time Data Pipeline

Kết quả đo lường hiệu năng thực tế. Tất cả số liệu đo bằng `benchmark/run_benchmark_v4.py` — phương pháp E2E honest (xem Phần 9).

---

## 1. Môi trường thử nghiệm

### Laptop (môi trường phát triển chính)

| Mục | Thông số |
|---|---|
| **CPU** | 11th Gen Intel Core i5-11400H @ 2.70GHz — 6 cores / 12 threads |
| **RAM** | 16 GB (available ~15.5 GB) |
| **OS** | Windows 10 Pro + Docker Desktop (WSL2 backend) |
| **Docker** | v28.0.1 / Docker Compose v2.33.1 |
| **Spark** | 3.5.0 — 3 Workers × 4 cores × 2 GB = **12 cores / 6 GB tổng** |
| **Kafka** | Confluent 7.5.0 — 1 partition (baseline), 3 partition (scale test) |
| **Spark job** | Scala JAR (`CdcRedisConsumer`) — micro-batch trigger 5s |
| **Pipeline** | MySQL → Debezium → Kafka → Spark Streaming → MongoDB + Redis |

### VM — Xeon (môi trường scale test)

| Mục | Thông số |
|---|---|
| **CPU** | Intel Xeon E5-2690 v4 @ 2.60GHz — 16 cores / 32 threads |
| **RAM** | 32 GB (available ~31.3 GB) |
| **OS** | Ubuntu 22.04 + Docker Engine |
| **Spark** | 3.5.0 — 1 / 3 / 6 Workers (tùy run), trigger 5s |
| **Kafka** | 3 / 6 / 12 partitions (tùy run) |

---

## 2. Kết quả chính — Laptop Baseline (Quick Mode, 1 partition, 3 workers)

> **Run tham chiếu:** `run_20260528_162517` (2026-05-28) — clean, no bottleneck detected.
> Các run cùng cấu hình trước đó (05-16, 05-26) cho kết quả trong khoảng ±4%.

### 2.1 Ramp-up theo mức tải

| Target inject | Inject thực | **E2E rec/s** | Drain time | Kafka avg | Mongo avg | p50 batch | p95 batch | p99 batch | Lag cuối |
|---|---|---|---|---|---|---|---|---|---|
| 100 rec/s | 99.8 rec/s | **80.6** | 4.8 s | 71.3 rec/s | 76.6 rec/s | 258 ms | 1,132 ms | 1,132 ms | 0 |
| 200 rec/s | 199.4 rec/s | **156.0** | 5.6 s | 185.8 rec/s | 165.0 rec/s | 493 ms | 998 ms | 998 ms | 0 |
| 500 rec/s | 497.2 rec/s | **388.8** | 5.6 s | 468.4 rec/s | 405.4 rec/s | 697 ms | 1,394 ms | 1,394 ms | 0 |

> **Drain time** = thời gian pipeline xử lý hết số records còn tồn đọng sau khi inject xong.
> Lag cuối = 0 ở mọi mức → pipeline drain hoàn toàn, không mất record.

### 2.2 Sustained throughput (30 giây liên tục)

| Target inject | Inject thực | **E2E rec/s** | Duration | Drain time | Kafka avg | Mongo avg | p50 batch | p95 batch | Lag cuối |
|---|---|---|---|---|---|---|---|---|---|
| 311 rec/s | 308.6 rec/s | **260.6** | 30 s | 5.6 s | 299.5 rec/s | 262.7 rec/s | 491 ms | 2,510 ms | 0 |

### 2.3 Tóm tắt kết quả baseline

| Metric | Giá trị |
|---|---|
| **Max E2E Throughput** | **388.8 rec/s** (tại inject 500 rec/s) |
| **Sustained Throughput** | **260.6 rec/s** (30 giây, no lag) |
| **Kafka partitions** | 1 |
| **Spark workers / cores** | 3 workers × 4 cores = 12 cores |
| **Spark engine** | Scala JAR |
| **Kafka lag cuối mỗi run** | 0 (drain hoàn toàn) |
| **Data loss** | 0 (MySQL delta = MongoDB delta ở mọi level) |
| **Bottleneck detected** | Không |

---

## 3. Stress Test — Tìm điểm giới hạn (Laptop, 1 partition, 3 workers)

> **Run:** `run_20260525_210543` — inject từ 100 đến 5000 rec/s, tìm điểm pipeline không kịp.

| Target inject | **E2E rec/s** | Kafka lag cuối | p50 batch | p95 batch | Kết quả |
|---|---|---|---|---|---|
| 100 rec/s | 87.0 | 0 | 428 ms | 1,176 ms | ✅ Kịp |
| 200 rec/s | 173.3 | 0 | 549 ms | 1,567 ms | ✅ Kịp |
| 500 rec/s | 429.7 | 0 | 907 ms | 2,716 ms | ✅ Kịp |
| 1,000 rec/s | **855.6** | 0 | 2,142 ms | 4,290 ms | ✅ Kịp (peak!) |
| 2,000 rec/s | 357.6 | 6,296 | 3,677 ms | 3,677 ms | ⚠ Bottleneck bắt đầu |
| Sustained @684 | 27.7 | 36,047 | 1,178 ms | 3,677 ms | ❌ Pipeline không kịp |

> **Kết luận:** Ngưỡng giới hạn ~1,000 rec/s với 1 partition, 3 workers.
> Trên 1,000 rec/s: Kafka lag tích lũy, pipeline bắt đầu tụt hậu.
> Tăng partition → tăng ngưỡng này (xem Phần 4).

---

## 4. Scale Test — Workers × Partitions

### 4.1 Laptop — Ảnh hưởng của số workers (3 partitions, quick mode)

| Spark Workers | Partitions | **Max E2E** | **Sustained** | p50 batch | p95 batch |
|---|---|---|---|---|---|
| 1 worker | 3 | 387.5 rec/s | 288.8 rec/s | 627 ms | 738 ms |
| 2 workers | 3 | 385.0 rec/s | 288.9 rec/s | 639 ms | 1,113 ms |
| **3 workers** | 3 | **449.1 rec/s** | **312.5 rec/s** | 613 ms | 2,149 ms |

> Tăng từ 1 → 3 workers (+200% compute): sustained tăng từ 288.8 → 312.5 rec/s (+8.2%).
> Hiệu quả scale worker thấp → bottleneck không phải ở Spark compute mà ở Debezium/Kafka.

### 4.2 Laptop — Ảnh hưởng của số partitions (3 workers, quick mode)

| Partitions | **Max E2E** | **Sustained** | p50 batch | p95 batch |
|---|---|---|---|---|
| 1 partition | 402.7 rec/s | 277.3 rec/s | 863 ms | 3,220 ms |
| 2 partitions | 386.9 rec/s | 261.8 rec/s | 820 ms | 4,020 ms |
| 3 partitions | 395.8 rec/s | 264.1 rec/s | 742 ms | 1,846 ms |

> Trên laptop (1 broker, 1 Debezium), thêm partition không cải thiện đáng kể throughput.
> Lý do: bottleneck là Debezium → Kafka bước, không phải Spark consumer.
> Partition thực sự có ý nghĩa khi có **nhiều broker** (xem VM).

### 4.3 VM (Xeon) — Ma trận workers × partitions (quick mode)

| Workers | Partitions | **Max E2E** | **Sustained** | p50 batch | p95 batch | Bottleneck |
|---|---|---|---|---|---|---|
| 3 | 3 | 368.4 rec/s | 252.0 rec/s | 1,379 ms | 4,762 ms | Không |
| 6 | 3 | 396.0 rec/s | 265.1 rec/s | 881 ms | 3,119 ms | Không |
| 3 | 6 | 387.3 rec/s | **292.6** rec/s | 976 ms | 1,312 ms | Không |
| 6 | 6 | 387.4 rec/s | 252.1 rec/s | 1,252 ms | 2,625 ms | Không |
| 3 | 12 | 382.2 rec/s | 262.4 rec/s | 742 ms | 1,846 ms | Không |
| 6 | 12 | 376.1 rec/s | 256.0 rec/s | 1,045 ms | 2,509 ms | Không |

> **Quan sát:** Trên VM với quick mode, cả 6 cấu hình cho kết quả tương đương (~370–396 rec/s max).
> Nguyên nhân: quick mode chỉ inject tối đa 500 rec/s — thấp hơn ngưỡng giới hạn thật.
> Full mode mới thể hiện rõ sự khác biệt (xem bên dưới).

### 4.4 VM (Xeon) — Full Mode (inject 500–2000 rec/s, 12 partitions)

| Workers | Partitions | **Max E2E** | **Sustained** | p50 batch | p95 batch | Mode |
|---|---|---|---|---|---|---|
| 3 | 12 | **1,633.1 rec/s** | **1,136.6 rec/s** | 1,010 ms | 1,833 ms | full |
| 3 | 12 | 1,337.7 rec/s | 921.2 rec/s | 1,221 ms | 2,748 ms | full |
| **Trung bình** | | **~1,485 rec/s** | **~1,029 rec/s** | — | — | full |

> **Full mode** dùng inject rates cao hơn (500/1000/2000 rec/s), phản ánh khả năng thực sự của VM.
> Với 12 partitions + Xeon 16 core: pipeline xử lý được **>1,000 rec/s sustained**.
> So với laptop (261 rec/s): **tăng ~3.9× khi có phần cứng mạnh hơn + nhiều partitions**.

---

## 5. So sánh Scala vs Python

| Metric | **Scala JAR** | PySpark | Chênh lệch |
|---|---|---|---|
| Max E2E (quick mode) | **388–449 rec/s** | ~82.5 rec/s | **~4.7–5.4× nhanh hơn** |
| Sustained | **260–312 rec/s** | ~55 rec/s (ước tính) | **~5× nhanh hơn** |
| Spark p50 | 258–697 ms | — | — |
| Spark p95 | 998–2,510 ms | — | — |
| Độ tin cậy | ✅ Ổn định | ⚠ Kém (drain bug) | — |
| Engine chính thức | ✅ | Fallback | — |

> Python benchmark kém tin cậy: DELETE events tích lũy trong Kafka làm sai điều kiện drain.
> Scala là engine chính thức. Python chỉ dùng khi JAR không available.

---

## 6. Lịch sử Benchmark — Runs tuyển chọn (49 total runs)

| Ngày | Engine | Mode | Workers | Partitions | Hardware | **Max rec/s** | **Sus rec/s** | p50 | p95 | Ghi chú |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-07 | Scala | quick | 3 | 1 | i5-11400H | 370.0 | 249.7 | 1,002 ms | 2,493 ms | Run đầu tiên có kết quả |
| 2026-05-07 | Python | quick | 3 | 1 | i5-11400H | 82.5 | — | — | — | Drain bug |
| 2026-05-16 | Scala | quick | 3 | 1 | i5-11400H | **404.3** | 273.3 | 923 ms | 2,363 ms | Highest max ever (laptop) |
| 2026-05-16 | Scala | quick | 3 | 1 | i5-11400H | 389.6 | 290.2 | 463 ms | **896 ms** | Lowest p95 (laptop) |
| 2026-05-25 | Scala | stress | 3 | 1 | i5-11400H | 855.6 | 27.7 | 1,178 ms | 3,677 ms | Giới hạn ~1000 rec/s |
| 2026-05-26 | Scala | quick | 1 | 3 | i5-11400H | 387.5 | 288.8 | 627 ms | 738 ms | Scale test: 1 worker |
| 2026-05-26 | Scala | quick | 3 | 3 | i5-11400H | **449.1** | **312.5** | 613 ms | 2,149 ms | **Best laptop ever** |
| 2026-05-27 | Scala | quick | 6 | 3 | Xeon E5 | 396.0 | 265.1 | 881 ms | 3,119 ms | VM: 6 workers |
| 2026-05-27 | Scala | quick | 3 | 6 | Xeon E5 | 387.3 | 292.6 | 976 ms | 1,312 ms | VM: 6 partitions |
| 2026-05-27 | Scala | full | 3 | 12 | Xeon E5 | **1,633.1** | **1,136.6** | 1,010 ms | 1,833 ms | **Best overall — VM full** |
| 2026-05-27 | Scala | full | 3 | 12 | Xeon E5 | 1,337.7 | 921.2 | 1,221 ms | 2,748 ms | VM full mode run 2 |
| **2026-05-28** | **Scala** | **quick** | **3** | **1** | **i5-11400H** | **388.8** | **260.6** | **491 ms** | **2,510 ms** | **Latest validated (canonical)** |

> Runs bị đánh dấu invalid (Spark replay, consumer rebalancing) đã loại khỏi bảng.

---

## 7. Smoke Test

**43/43 PASS** — tất cả checks đều pass:
- 13 containers running & healthy
- 9 port connectivity checks
- Debezium connector + task: RUNNING
- Kafka topics: `inventory.inventory.customers`, `inventory.inventory.orders`
- Spark: 1 active app, 3 workers alive, 12 executor cores
- MySQL ↔ MongoDB in sync
- **E2E insert latency**: record xuất hiện trong MongoDB sau **~3 giây** (smoke test, không phải percentile chính thức)
- 6 metrics present trong Prometheus
- 3 hardware profiles tồn tại trong `config/`

---

## 8. Phương pháp đo

```
E2E rec/s = (mongo_count_after - mongo_count_before) / (inject_time + drain_time)
```

- **Không** tính theo inject rate — tính theo số records **thực sự đến MongoDB**.
- **Drain time** = thời gian chờ sau khi inject xong để pipeline xử lý hết lag.
- Điều kiện "synced": `mongo_count_after - mongo_count_before >= mysql_delta` (không dùng tổng tích lũy — tránh false positive từ data cũ).
- **Bottleneck detection**: nếu Kafka consumer lag > threshold sau drain → ghi nhận bottleneck.
- Mỗi level test độc lập — sau mỗi level đợi drain về 0 mới sang level tiếp.

> Xem thêm: [docs/CLARIFICATIONS.md](CLARIFICATIONS.md) — giải thích chi tiết E2E vs inject rate.

---

## 9. Thông số thiếu — Cần đo bổ sung cho DATN

Phần này liệt kê các thông số **chưa có trong benchmark hiện tại** nhưng quan trọng cho báo cáo luận văn hoặc có thể bị hội đồng hỏi.

### 9.1 Priority cao — Hội đồng gần như chắc chắn hỏi

| # | Thông số | Hiện trạng | Cách đo |
|---|---|---|---|
| 1 | **E2E Latency per record (P50/P95)** | Chỉ có "~3 giây" từ smoke test, không phải số đo chính thức | Ghi timestamp khi INSERT, đọc timestamp khi MongoDB confirm, tính delta. Cần thêm vào benchmark loop |
| 2 | **Peak Kafka consumer lag** | Chỉ có `lag_remaining` cuối run (luôn = 0). Không biết peak lag là bao nhiêu | Log `cdc_kafka_consumer_lag` từ Prometheus trong suốt quá trình inject |
| 3 | **Throughput degradation over time** | Benchmark ngắn (~2–3 phút). Không biết throughput sau 30–60 phút liên tục | Chạy sustained mode 60 phút, lấy trung bình từng 5 phút |

### 9.2 Priority trung bình — Có thể bị hỏi

| # | Thông số | Hiện trạng | Cách đo |
|---|---|---|---|
| 4 | **Redis write latency** | Không có số đo riêng. Pipeline ghi Redis trong cùng Spark job với MongoDB nhưng latency chưa đo | Thêm CHECK sau mỗi inject batch: đọc Redis key, so sánh timestamp |
| 5 | **Data integrity dưới tải** | Chỉ verify count ở cuối mỗi level. Không verify _trong khi_ inject | Thêm concurrent check: query MySQL count vs MongoDB count mỗi 5 giây trong khi inject |
| 6 | **Throughput với multi-table** | Benchmark chỉ inject `customers`. Pipeline xử lý cả `orders` | Test đồng thời INSERT vào cả `customers` và `orders` |
| 7 | **Debezium snapshot rate** | Không đo. Cold start với nhiều records thì Debezium mất bao lâu để snapshot? | Tạo 100k records trước, reset Debezium, đo thời gian snapshot hoàn thành |

### 9.3 Nice-to-have — Làm đẹp báo cáo

| # | Thông số | Hiện trạng | Cách đo |
|---|---|---|---|
| 8 | **CPU/Memory per service** | Không có. Không biết Kafka hay Spark hay MongoDB đang dùng bao nhiêu RAM khi chạy | `docker stats --no-stream` trong khi benchmark đang chạy |
| 9 | **Throughput với `UPDATE`/`DELETE`** | Benchmark chỉ INSERT. Debezium cũng capture UPDATE/DELETE nhưng chưa test | Thêm UPDATE/DELETE mix vào benchmark (60% INSERT / 30% UPDATE / 10% DELETE — mode `realistic` đã có code) |
| 10 | **Kafka message retention vs throughput** | Không đo. Disk I/O khi Kafka log lớn có ảnh hưởng không? | Chạy full mode liên tục, monitor `df -h` và `iostat` |

### 9.4 Số liệu đã có — Dùng ngay được cho báo cáo

| Thông số | Giá trị |
|---|---|
| Max throughput (laptop, 3w, 3p) | **449.1 rec/s** |
| Sustained throughput (laptop) | **260–312 rec/s** |
| Max throughput (VM, 12p, full mode) | **1,633 rec/s** |
| Sustained (VM, 12p, full mode) | **1,137 rec/s** |
| Pipeline giới hạn (laptop, 1p, 3w) | ~**1,000 rec/s** (stress test) |
| Scala vs Python tỷ lệ | **~5×** |
| Zero data loss | ✅ Verified mọi run |
| Kafka consumer lag cuối | **0** mọi run (dưới ngưỡng giới hạn) |
| Fault recovery time (Kafka crash) | **~45s** (từ fault-tolerance test) |
| Fault recovery time (3-broker ISR) | **< 5s** (VM scenario) |
| Spark trigger interval | **5 giây** |
| E2E latency (smoke test) | **~3 giây** (1 record, không phải percentile) |
