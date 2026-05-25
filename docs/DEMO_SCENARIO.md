# Kịch bản Demo — CDC Real-time Data Pipeline

---

## Phần 0 — Phân tích số liệu thực tế (cơ sở xây dựng bài toán)

### 0.1 Số liệu từ các công ty thật — Việt Nam

#### Thương hiệu thời trang Việt Nam có quy mô gần với VietShop

| Công ty | Doanh thu | Kênh | Đơn ước tính/ngày | Ghi chú |
|---------|-----------|------|--------------------|---------|
| **Owen** | VND 341 tỷ (~$14M) năm 2022 | 180+ cửa hàng vật lý + online | ~1,000–2,000 | Thương hiệu thời trang nam, website riêng |
| **Canifa** | VND 1,400 tỷ (~$56M) năm 2022 | 100+ cửa hàng + online | ~5,000–10,000 | Top 3 Shopee VN, top 2 Lazada VN |
| **VietShop (kịch bản này)** | **~VND 600 tỷ ($25M)** | **Online-first** | **~4,000–6,000** | **Tính ngược từ GMV (xem 0.2)** |

> **Nguồn Owen & Canifa:** [Vietnamese fashion brands earn high revenue but modest profit — VietnamNet](https://vietnamnet.vn/en/vietnamese-fashion-brands-earn-high-revenue-but-modest-profit-598586.html) · [Canifa đạt Thương hiệu Quốc gia 2024](https://canifa.com/blog/canifa-tro-thanh-thuong-hieu-quoc-gia-viet-nam-2024) · [CleverTap Canifa case study — Nasdaq](https://www.nasdaq.com/press-release/leading-fashion-brand-canifa-achieves-54-higher-app-conversion-rate-with-clevertap)

**Nhận xét:** Canifa ($56M GMV, 100+ stores) là benchmark phù hợp nhất. VietShop được đặt ở **~$25M GMV, online-only** — nhỏ hơn Canifa nhưng cùng ngành và cùng cần real-time inventory sync.

#### Fintech Việt Nam: MoMo (tham khảo về real-time transaction)

MoMo có **31–40 triệu users** (2023–2024), xử lý **hàng triệu giao dịch/ngày**, chiếm 68% thị phần ví điện tử VN. Đây là ví dụ thực tế gần nhất của VN về hệ thống cần real-time data pipeline tương tự CDC.

> **Nguồn:** [MoMo super app 2024 — AgileTech](https://agiletech.vn/momo-e-wallet-super-app-platform/) · [Vietnam mobile payments — Transfi](https://www.transfi.com/blog/vietnams-top-payment-methods-momo-zalopay-vietqr-explained)

#### Shopee Vietnam: Bức tranh toàn cảnh (tham khảo bậc trên)

| Metric | Số liệu | Nguồn |
|--------|---------|-------|
| Thị phần GMV | 65–67% (~$9.3–10.4B) | Statista Q1 2024 |
| Số sellers tại VN | 262,000+ (Q1 2024) | Statista |
| Avg GMV/seller/quý | $8,658 | Statista |
| Peak 11.11 (SEA) | **11M+ đơn/24h** | Nation Thailand |
| Peak 11.11 (SEA 2021) | **200M sản phẩm/24h** | Digital News Asia |

> Shopee ở tầng khác hoàn toàn — hàng nghìn Kafka brokers, distributed cluster thực sự. VietShop ở tầng "brand vừa cần CDC để không bị overwhelmed khi scale."
>
> **Nguồn Shopee:** [Shopee achieves over 11M orders in 24h — Nation Thailand](https://www.nationthailand.com/business/30358626) · [Shopee 11.11 200M items — Digital News Asia](https://www.digitalnewsasia.com/business/shopees-1111-big-sale-saw-200-million-items-sold-one-day-across-sea)

---

### 0.2 Số liệu từ các công ty thật — Nước ngoài (cùng ngành thời trang)

| Công ty | Quốc gia | Đơn hàng/năm | **Đơn/ngày (avg)** | Active customers | Revenue |
|---------|----------|-------------|---------------------|-----------------|---------|
| **Zalando** | Germany | 251M (2024) | **~688,000** | 51.8M | €10.6B |
| **ASOS** | UK | ~40M (ước tính 2022) | **~110,000** | ~26M | £3.54B |
| **Myntra** | India | — | **~200,000–400,000** | 60M MAU | ₹43.75B (~$527M) |

> - **Zalando:** [Annual Report 2024](https://corporate.zalando.com/en/investor-relations/annual-report-2024) · 244.8M orders (2023) → 251M (2024), tăng 2.5% YoY. Profit tăng 200% lên €251M.
> - **ASOS:** 99.7M items ordered (2022), avg ~2.5 items/order → ~40M đơn/năm → ~110K/ngày. Revenue £3.54B (FY2023). [ASOS Annual Report 2023](https://asos-12954-s3.s3.eu-west-2.amazonaws.com/files/7217/0065/7934/ASOS_Annual_Report_2023.pdf)
> - **Myntra (India):** EORS (End of Reason Sale) 6 ngày = 5.5M đơn → ~917K/ngày khi sale; bình thường ước ~200K–400K/ngày. Revenue FY2023 = ₹43.75B. [Myntra data analysis — Vumonic](https://www.vumonic.com/blog/a-data-driven-deep-dive-into-myntras-fashion-frenzy)

**CDC events/s tương ứng (ước tính, 6 events/đơn):**

| Công ty | Đơn/ngày avg | CDC events/s avg | CDC events/s peak (5× sale) |
|---------|-------------|------------------|-----------------------------|
| Zalando | 688,000 | ~48 | ~240 |
| ASOS | 110,000 | ~8 | ~40 |
| Myntra | 300,000 | ~21 | ~106 (EORS) |
| **VietShop (kịch bản này)** | **4,500** | **~0.3** | **~17 (flash sale)** |
| **Pipeline này (đo thực)** | — | **275 sustained** | **404 peak** |

> Zalando xử lý ~48 CDC events/s trung bình — pipeline này (275 rec/s) đủ xử lý **5.7× Zalando trung bình** trên một node Docker localhost. Điều này cho thấy 275 rec/s là một con số có ý nghĩa thực tế, không phải benchmark "ảo".

---

### 0.3 Tính ngược: VietShop ở đâu trong spectrum này?

**Cách xác định quy mô hợp lý cho VietShop:**

```
Tham chiếu Canifa: VND 1,400 tỷ ($56M), 100+ stores → ~5,000–10,000 đơn/ngày
VietShop: online-first, không có cửa hàng vật lý, nhỏ hơn Canifa

→ VietShop đặt ở $25M GMV (bằng ~45% GMV Canifa, không tính offline)

Tính ngược đơn hàng:
  $25M GMV ÷ $12 avg order (VN online fashion) = 2.08M đơn/năm
  → 2,080,000 ÷ 365 ≈ 5,700 đơn/ngày ✓
  (Điều chỉnh cho flash sale vs weekday: 4,000–6,000 bình thường, peak 30,000–35,000)
```

**Tính số khách hàng đăng ký:**
```
5,700 đơn/ngày × 30 ngày = 171,000 đơn/tháng
Avg 3 đơn/tháng/khách active + tỷ lệ active 35–40%
→ 171,000 ÷ 3 ÷ 0.37 ≈ 154,000 → làm tròn ~120,000–150,000 accounts
```

→ **VietShop hợp lý: ~120,000 khách đăng ký, 4,000–6,000 đơn/ngày, $25M GMV/năm**

So sánh trực quan:

```
Đơn/ngày (log scale):
  Owen       [1,000–2,000]  ──────
  VietShop   [4,000–6,000]  ──────────── ← kịch bản này
  Canifa     [5,000–10,000] ────────────────
  Myntra     [200,000–400K] ════════════════════════════════
  ASOS       [~110,000]     ═════════════════════════
  Zalando    [688,000]      ═══════════════════════════════════════════════
  Shopee VN  [2.9M]         █████████████████████████████████████████████████
```

---

### 0.4 Từ đơn hàng → CDC events/s (số quan trọng nhất)

Mỗi đơn hàng tạo ra nhiều MySQL operations, mỗi operation = 1 CDC event:

| Operation | MySQL table | CDC event |
|-----------|-------------|-----------|
| Khách đặt hàng | `orders` | INSERT (op=c) |
| 2–4 mặt hàng | `order_items` | 2–4 × INSERT |
| Cập nhật tồn kho | `inventory` | UPDATE (op=u) |
| Ghi thanh toán | `payments` | INSERT (op=c) |

→ **1 đơn = ~5–7 CDC events trung bình**

**Tải CDC của VietShop theo thời điểm:**

| Thời điểm | Đơn hàng | Đơn/giây | CDC events/s |
|-----------|----------|----------|--------------|
| Ngoài giờ cao điểm | 100–200/h | 0.03–0.06 | 0.2–0.4 |
| Giờ cao điểm (10h–22h) | 500–600/h | 0.14–0.17 | 0.9–1.2 |
| Flash sale (4h đầu) | 7,500–8,000/h | 2.1–2.2 | 12.5–15 |
| Burst đầu flash sale (15 phút) | 2,500–3,000 | 2.8–3.3 | **~17–20** |

**So sánh với pipeline này (đo thực):**

```
VietShop peak cần:    ~17–20 events/s
Pipeline sustained:   275 rec/s
Pipeline peak:        404 rec/s

Headroom: 275 ÷ 17 ≈ 16× → VietShop tăng trưởng 16× mà không cần scale infra
```

---

## Phần 1 — Bài toán từ thực tế, thông số VietShop

### Thông số thực tế của pipeline này (đo bằng `bash run_bench.sh`, Scala mode)

| Metric đo được | Giá trị | Ghi chú |
|---|---|---|
| E2E throughput (peak) | **404 rec/s** | MySQL → MongoDB, Scala mode |
| E2E throughput (sustained) | **~275 rec/s** | trung bình qua nhiều batch |
| Spark batch duration p50 | **~650ms** | median xử lý 1 micro-batch |
| Spark batch duration p95 | **~900–2500ms** | tùy tải |
| Kafka rate | **290–450 events/s** | Debezium → Kafka |
| E2E latency (avg) | **~2.5s** | do trigger 5s |
| E2E latency (worst case) | **~5s** | record insert ngay trước trigger |
| Recovery sau Spark crash | **~15–30s** | resubmit + checkpoint resume |

> **Môi trường:** i5-11400H, 32GB RAM, Docker Desktop, single-node, Kafka RF=1. Network latency ≈ 0ms (localhost). Đây là số đo thực tế từ `benchmark/results/history.jsonl`.

---

### Bối cảnh: "VietShop" — thương hiệu thời trang đang scale nhanh

**VietShop** — 3 năm tuổi, bán thời trang + phụ kiện online, ~120,000 khách hàng đăng ký, xử lý **4,000–6,000 đơn/ngày** (~$25M GMV/năm). Trong 6 tháng gần nhất tăng trưởng 3× (từ 1,500 lên 5,000 đơn/ngày).

> **Tại sao 120,000 khách hàng?** Xem phân tích Phần 0.2: 4,500 đơn/ngày × 30 ngày ÷ 3 đơn/tháng/khách active ÷ tỷ lệ active 35% ≈ 128,000 registered accounts.

**Hệ thống ban đầu:** MySQL duy nhất phục vụ tất cả:

```
App bán hàng (web/mobile)  ──┐
App quản lý kho            ──┤──► MySQL (OLTP)   ← mọi thứ đọc chung
Dashboard realtime         ──┤
Team Data / báo cáo        ──┘
```

**Vấn đề phát sinh khi scale từ 1,500 lên 5,000 đơn/ngày:**

| Thời điểm | Triệu chứng | Hệ quả kinh doanh |
|---|---|---|
| Bình thường | Query báo cáo nặng → lock row | Khách chờ 3–5s để checkout |
| Flash sale (30,000 đơn/ngày) | 6× traffic đột ngột → MySQL timeout | Đơn bị mất, khách không nhận xác nhận |
| Cuối ngày | ETL batch chạy 90 phút → lock table | Không cập nhật được tồn kho trong 1.5h |
| Mỗi sáng | Báo cáo phải chờ ETL xong | Quản lý không có số liệu buổi sáng |

**Câu hỏi cụ thể từ CTO VietShop:**
> "Dashboard quản lý đang delay 1–2 giờ vì ETL batch. Khách đặt hàng xong mà kho chưa thấy — đã oversell 3 lần tháng này. Có cách nào để kho và dashboard thấy đơn **ngay khi nó được tạo**, mà không phải sửa app bán hàng?"

---

## Phần 2 — Tại sao CDC, không phải giải pháp khác?

### So sánh các phương án

| Phương án | Cơ chế | Vấn đề với VietShop |
|---|---|---|
| **Polling** (30s query MySQL) | `SELECT * WHERE updated_at > ?` | Bỏ sót DELETE; tải thêm lên MySQL đang quá tải; delay 0–30s |
| **Dual-write** (app ghi 2 DB) | Code ghi MySQL + MongoDB song song | Phải sửa tất cả code app; race condition; MySQL fail ≠ rollback MongoDB |
| **ETL batch** (Airflow 1h/lần) | Export → Transform → Load | Đúng vấn đề VietShop đang gặp — delay 1h, oversell khi flash sale |
| **CDC (giải pháp này)** | Đọc MySQL binary log | Zero thay đổi app; bắt INSERT/UPDATE/DELETE; delay 2–5s |

**CDC đọc MySQL binlog** — giống đọc "nhật ký giao dịch" của database. App bán hàng không biết gì, không cần sửa một dòng code.

---

## Phần 3 — Kiến trúc

### Sơ đồ

```
┌─────────────────────────────────────────────────────────────┐
│  App bán hàng VietShop                                      │
│  INSERT/UPDATE/DELETE → MySQL 8.0 (inventory DB)            │
│                         binlog_format = ROW                 │
└────────────────────┬────────────────────────────────────────┘
                     │ Binary Log (binlog)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Debezium 2.5 — MySQL Source Connector                      │
│  Đọc binlog, phát JSON event: { op: c/u/d/r, before, after }│
└────────────────────┬────────────────────────────────────────┘
                     │ JSON events
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Kafka (Confluent 7.5.0)                                    │
│  Topic: inventory.inventory.customers                       │
│  Topic: inventory.inventory.orders                          │
│  Buffer tối đa 7 ngày — pipeline crash không mất event     │
└────────────────────┬────────────────────────────────────────┘
                     │ Kafka messages (offset-tracked)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Spark Structured Streaming 3.5.0                           │
│  Trigger: 5s/batch │ Workers: 3 × (2 core, 1GB)            │
│  op=c/r → upsert MongoDB + INCR Redis                       │
│  op=u   → upsert MongoDB (không đổi counter)               │
│  op=d   → delete MongoDB + DECR Redis                       │
│  Checkpoint: /tmp/spark-checkpoint/ (resume sau crash)      │
└──────────┬──────────────────────────┬───────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐       ┌──────────────────────────────────┐
│  MongoDB 7.0     │       │  Redis 7                         │
│  customers (coll)│       │  customers:total → số KH thực   │
│  orders (coll)   │       │  customer:{id}   → hash chi tiết │
│                  │       │  orders:total    → số đơn hàng  │
│  → Analytics     │       │  → Dashboard realtime <1ms read │
│  → Báo cáo       │       │  → API check tồn kho nhanh      │
│  → ML features   │       └──────────────────────────────────┘
└──────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│  Prometheus + Grafana — Observability                       │
│  metrics_exporter scrape mỗi 5s:                           │
│  lag_total, kafka_consumer_lag, spark_batch_duration_ms    │
│  mysql_count, mongo_count, redis_customers_total           │
└─────────────────────────────────────────────────────────────┘
```

### Lý do chọn từng công nghệ

| Quyết định | Lý do cụ thể |
|---|---|
| **Debezium** thay vì tự parse binlog | Xử lý sẵn: schema evolution, snapshot, offset recovery — không reinvent |
| **Kafka** làm buffer | Nếu Spark xử lý chậm hơn inject rate → Kafka giữ message, không mất. Replay được sau crash |
| **Spark Structured Streaming** | Exactly-once semantics + checkpoint → không mất data, không duplicate khi restart |
| **MongoDB** cho analytics | Document model linh hoạt; read-heavy phù hợp; không lock như OLTP MySQL |
| **Redis** cho realtime | Sub-millisecond read; counter INCR/DECR atomic; dashboard cần <100ms response |
| **Trigger 5s** | Trade-off có chủ ý: latency ~2.5s avg là chấp nhận được với VietShop; trigger nhỏ hơn tăng overhead không đáng |

---

## Phần 4 — Demo (Input → Output rõ ràng)

### Chuẩn bị (5 phút trước demo)

```bash
bash start.sh          # Khởi 12 container (~3-5 phút)
cd demo
python demo_server.py  # http://localhost:8888
# Mở thêm: http://localhost:3000 → Grafana
```

---

### Scene 1 — "Khách hàng mới đăng ký" (2 phút)

**Bối cảnh:** Một khách hàng vừa điền form đăng ký trên website VietShop. App INSERT vào MySQL. Dashboard kho cần thấy ngay.

**Input** — Dùng form "Scene 1" trong web demo (http://localhost:8888):
```
Tên:   Nguyễn Thị Mai
Email: mai.nguyen@gmail.com
SĐT:   0912345678
→ Nhấn: [▶ INSERT → WATCH]
```

**Luồng xử lý hiện ra ngay trên web:**
```
[✓] MySQL INSERT       → 12ms
[✓] Debezium binlog    → ~50ms
[✓] Kafka event        → ~80ms
[⟳] Spark batch (5s trigger)...
[✓] MongoDB synced     → 4.200s

E2E Latency: 4.20s
```

**Output quan sát:**
- Web demo: thanh pipeline xanh hết từng bước + E2E hiện số giây thật
- Grafana: MySQL customers +1, MongoDB customers +1, lag về 0

**Điểm nhấn:** Không sửa một dòng code app bán hàng. Pipeline tự detect INSERT từ binlog.

---

### Scene 2 — "Flash Sale: Stress test 100 rec/s" (3 phút)

**Bối cảnh:** VietShop chạy flash sale. Pipeline xử lý được bao nhiêu?

> **Lưu ý về con số:** Trong thực tế VietShop peak ~17–20 CDC events/s (xem Phần 0.3). Demo inject 100 rec/s = **5× VietShop peak** — đây là stress test chủ động để chứng minh headroom, không phải mô phỏng 1:1.

**Input** — Web demo, chọn rate và nhấn START:
```
Rate:   100 rec/s
Action: [▶ START DEMO]
Inject: ~500 customers + ~300 orders tự động
```

**Output realtime trên Grafana + web:**

| Metric | Thấy được | Ý nghĩa |
|---|---|---|
| MySQL customers | Tăng ~100/s | Records đang vào |
| MongoDB customers | Lag ~5s rồi bắt kịp | Spark đang xử lý batch |
| Kafka consumer lag | Spike lên ~50–200 | Kafka nhận nhanh hơn Spark xử lý |
| Spark batch duration | 650–2500ms | Spark đang chạy micro-batch |
| E2E lag | Tăng lúc inject, về 0 sau ~10s | Pipeline drain hết backlog |

**Kết quả benchmark thật** (đo bằng `bash run_bench.sh`, Scala mode, i5-11400H):
```
E2E peak:        404 rec/s
E2E sustained:   275 rec/s    ← 16× VietShop flash sale peak (17 events/s)
Spark p50 batch: 650ms
Spark p95 batch: 900–2500ms
Kafka rate:      290–450 events/s
Drain time:      ~8–12s sau khi dừng inject
```

**Điểm nhấn:** Kafka làm buffer — dù Spark xử lý ~275 rec/s, không có record nào bị mất. Kafka giữ message tối đa 7 ngày. Pipeline có **16× headroom** so với VietShop peak thực tế.

---

### Scene 3 — "Cập nhật trạng thái đơn hàng" (2 phút)

**Bối cảnh:** Đơn VietShop đi từ PENDING → PROCESSING → SHIPPED → DELIVERED. CDC phải bắt UPDATE, không tạo document mới trong MongoDB.

**Input** — Trong bảng Orders của web demo, nhấn nút **→** trên bất kỳ đơn hàng nào:
```
Đơn #5: PENDING → [→] → PROCESSING
Đơn #5: PROCESSING → [→] → SHIPPED
```

**Output:**
- MongoDB: document `orders/{id=5}` field `status` thay đổi — **không tạo document mới**
- Redis `orders:total`: **không thay đổi** (UPDATE không incr counter)
- Kafka: event với `op=u` (update, không phải `op=c` insert)
- Grafana: Kafka event count tăng 1, MongoDB count không đổi

**Điểm nhấn:** Debezium phân biệt `op=c` (INSERT) / `op=u` (UPDATE) / `op=d` (DELETE). Spark xử lý đúng từng loại → MongoDB không bị duplicate records.

---

### Scene 4 — "Fault Tolerance: Spark crash giữa chừng" (2 phút, tuỳ chọn)

**Bối cảnh:** Hội đồng hỏi: "Nếu server Spark bị lỗi giữa chừng thì sao?"

**Input** — Tab "Fault Tolerance" trong web demo:
```
Chọn: [⚙ Spark Driver Crash]
Nhấn: [▶ RUN SCENARIO]
```

**Output hiển thị trong Recovery Timeline:**
```
[SCENARIO] Spark Job Crash & Checkpoint Recovery
[BASELINE] MySQL=523 | MongoDB=523
[INJECT]   Killing Spark streaming job...
[STATUS]   Spark DOWN — messages accumulating in Kafka
[DATA]     1 record inserted → event in Kafka → waiting for restart
[RECOVER]  Re-submitting Spark job with checkpoint...
[STATUS]   Reading from checkpoint offset, no reprocessing
[RESULT]   Recovery: 28s | MySQL=524 | MongoDB=524 | PASS — 0 data loss
```

**Điểm nhấn:** Kafka giữ messages khi Spark down. Spark checkpoint lưu offset đã xử lý. Restart → resume từ đúng offset → exactly-once, không mất, không duplicate.

---

## Phần 5 — Thông số kỹ thuật + phân tích headroom

### Môi trường test

| Thông số | Giá trị |
|---|---|
| CPU | Intel Core i5-11400H (6 cores / 12 threads) |
| RAM | 32GB |
| Storage | SSD NVMe |
| Deployment | Docker Desktop, 12 containers, localhost |
| Network | Docker bridge ≈ 0ms (single machine) |
| Kafka | 1 broker, 1 partition, RF=1 |
| Spark | 1 Master + 3 Workers (2 core / 1GB each) |

### Kết quả đo được (từ benchmark thật, Scala mode)

| Metric | Giá trị đo được | Nguồn |
|---|---|---|
| E2E throughput (peak) | **404 rec/s** | `benchmark/results/history.jsonl` |
| E2E throughput (sustained) | **~275 rec/s** | trung bình các batch |
| Spark batch p50 | **~650ms** | `spark_p50_ms` |
| Spark batch p95 | **~900–2500ms** | `spark_p95_ms` |
| Kafka rate | **290–450 events/s** | `kafka_rate` |
| E2E latency (avg) | **~2.5s** | = trigger interval / 2 |
| E2E latency (worst) | **~5s** | = 1 full trigger cycle |
| Fault recovery time | **~15–30s** | đo thực tế từ demo |

### Pipeline capacity vs nhu cầu VietShop

| Scenario | VietShop cần (events/s) | Pipeline có (events/s) | Headroom |
|----------|------------------------|------------------------|---------|
| Giờ thường | 0.2–1.2 | 275 | 229–1375× |
| Giờ cao điểm | 0.9–1.2 | 275 | 229–305× |
| Flash sale peak | 17–20 | 275 | **14–16×** |
| Stress test demo | 100 | 275 | 2.75× |

**Headroom 16×** có nghĩa: VietShop cần tăng trưởng **16 lần** (từ $25M lên ~$400M GMV) trước khi cần scale thêm Kafka partition hoặc Spark worker. Tức là không cần thay đổi infra trong vòng 5–7 năm với tốc độ tăng trưởng hiện tại (3× mỗi 6 tháng → 16× sau ~3 năm).

### Tuyên bố trung thực khi hội đồng hỏi về con số

> **404 rec/s là throughput thật của setup single-node localhost.** Môi trường này có lợi thế không chủ ý: network latency ≈ 0ms, không có replication (Kafka RF=1, no MongoDB replica set), inject single-threaded từ 1 client.
>
> **Con số 100 rec/s trong demo** là stress test — gấp 5× nhu cầu peak thực tế của VietShop. Mục đích: chứng minh pipeline không bị overwhelmed ngay cả khi tải cao bất thường.
>
> **Điểm mạnh thực sự không phải con số tuyệt đối**, mà là **kiến trúc scale ngang**: thêm Kafka partition → thêm Spark worker → throughput tăng gần tuyến tính. Thêm Kafka broker + MongoDB replica set → fault tolerance tăng mà logic không đổi.
>
> **So sánh có ý nghĩa trong project này:** Scala JAR (~290 rec/s sustained) vs Python fallback (~82 rec/s) — cùng pipeline, cùng trigger 5s, cùng hardware. Sự chênh lệch này do JVM overhead của PySpark, không do logic.

### Đặt vào bối cảnh production thực (tham khảo)

| Hệ thống | Throughput | Infra |
|---|---|---|
| Debezium benchmark (đo thực) | 1,500 ops/s single MySQL table | 1 connector |
| **Pipeline này (đo thực)** | **275–404 rec/s** | **1-node Docker** |
| Zepto (grocery delivery, ~1M orders/day) | N/A public | Amazon MSK multi-cluster |
| Walmart online (1M transactions/day) | ~500M events/day | Large distributed |
| LinkedIn (CDC cho Espresso) | 4.5M msgs/s peak | Thousands of brokers |

> Pipeline này nằm trong range của **một production Debezium connector đơn lẻ** — đây là baseline hợp lý cho prototype single-node. Scale lên Tiki-level cần thêm Kafka partitions, Spark executors, và có thể Kafka MirrorMaker cho multi-region.

---

## Phần 6 — Timeline Demo 15 phút

```
[0:00]  Slide: Bài toán VietShop — tại sao ETL batch không đủ    2 phút
[2:00]  Sơ đồ kiến trúc + giải thích từng layer                  3 phút
[5:00]  Scene 1: Gõ tên → nhấn INSERT → thấy E2E latency thật    2 phút
[7:00]  Scene 2: Flash sale — Start 100/s → thấy Kafka lag spike  3 phút
[10:00] Scene 3: Nhấn → đổi status đơn hàng → thấy MongoDB update 2 phút
[12:00] Scene 4 (tuỳ chọn): Fault tolerance Spark crash          2 phút
[14:00] Tổng kết: headroom 16× + con số benchmark thật           1 phút
```
