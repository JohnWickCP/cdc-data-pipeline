# CDC Pipeline — MySQL → Kafka → Spark → MongoDB + Redis

Hệ thống **Change Data Capture (CDC)** đồng bộ dữ liệu real-time từ MySQL sang MongoDB và Redis thông qua Debezium + Kafka + Spark Structured Streaming, có monitoring bằng Prometheus + Grafana.

**→ Xem [SETUP.md](SETUP.md) để cài đặt và chạy.**

---

## Kiến trúc

```
┌─────────┐  binlog   ┌──────────┐  CDC events  ┌───────┐
│  MySQL  │ ────────► │ Debezium │ ────────────► │ Kafka │
└─────────┘           └──────────┘               └───┬───┘
                                                      │
                                        Spark Structured Streaming
                                         (Scala JAR hoặc PySpark)
                                                      │
                             ┌────────────────────────┴───────────────┐
                             ▼                                         ▼
                      ┌──────────┐                             ┌──────────┐
                      │ MongoDB  │  (lưu trữ, truy vấn)        │  Redis   │  (cache)
                      └──────────┘                             └──────────┘
                             │                                         │
                             └──────────────┬──────────────────────────┘
                                            │
                                  ┌─────────▼──────────┐
                                  │  metrics_exporter  │  :8000
                                  └─────────┬──────────┘
                                            │ scrape/5s
                                  ┌─────────▼──────────┐
                                  │  Prometheus :9090  │
                                  └─────────┬──────────┘
                                            │
                                  ┌─────────▼──────────┐
                                  │   Grafana :3000    │
                                  └────────────────────┘
```

---

## Kết quả benchmark (laptop i5-11400H, 12 cores, Scala JAR)

| Mức inject | E2E Throughput | Spark p50 | Spark p95 | Lag cuối |
|---|---|---|---|---|
| 100 rec/s | **92.7 rec/s** | 672 ms | 1,248 ms | 0 |
| 200 rec/s | **149.9 rec/s** | 669 ms | 1,088 ms | 0 |
| 500 rec/s | **404.3 rec/s** | 770 ms | 1,362 ms | 0 |
| Sustained (323 rec/s × 30s) | **273.3 rec/s** | 923 ms | 2,363 ms | 0 |

- Smoke test: **43/43 PASS**
- E2E insert → MongoDB: **~3 giây** (record xuất hiện sau 3s)
- Scala 4.9× nhanh hơn PySpark (~404 vs ~83 rec/s)
- **Không bottleneck** tại mọi mức test (Kafka lag = 0)

> Chi tiết đầy đủ: [docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) | Phương pháp đo: [docs/CLARIFICATIONS.md](docs/CLARIFICATIONS.md)

---

## Cấu trúc project

```
cdc-data-pipeline/
├── docker-compose.yml          # 12 containers
├── .env.laptop / .env.server / .env.vm   # Hardware profiles
├── start.sh                    # Khởi động pipeline (~3-5 phút)
├── stop.sh                     # Dừng pipeline
├── test_smoke.sh               # Smoke test 43 checks
├── run_bench.sh                # Chạy benchmark
├── SETUP.md                    # Hướng dẫn cài đặt và chạy
│
├── jobs/
│   ├── cdc-mysql-to-mongodb-redis_2.12-1.0.jar  # Scala JAR
│   ├── python/cdc_pipeline.py                   # PySpark fallback
│   └── scala/cdc_redis_consumer.scala            # Source Scala
│
├── demo/
│   ├── demo_server.py          # Live demo backend (Flask, :8888)
│   ├── index.html              # Dashboard UI
│   └── run_demo.sh / run_demo.bat
│
├── monitoring/
│   ├── exporter/metrics_exporter.py
│   ├── prometheus.yml
│   └── grafana/dashboards/cdc_fixed1.json
│
├── benchmark/
│   ├── run_benchmark_v4.py
│   ├── compare_runs.py
│   └── results/history.jsonl
│
└── docs/
    ├── BENCHMARK_RESULTS.md    # Kết quả đo lường hiệu năng đầy đủ
    ├── CLARIFICATIONS.md       # Giải thích khái niệm (TPS, records/s)
    ├── DEMO_SCRIPT.md          # Kịch bản demo cho hội đồng
    ├── KNOWN_ISSUES.md
    └── LESSONS_LEARNED.md
```
