# CDC Pipeline — MySQL → Kafka → Spark → MongoDB + Redis

Hệ thống **Change Data Capture (CDC)** real-time đồng bộ dữ liệu từ MySQL sang MongoDB và Redis qua Debezium + Kafka + Spark Streaming, có monitoring bằng Prometheus + Grafana.

![Architecture](screenshots/01-full-pipeline-running.png)

---

## Kiến trúc

```
┌─────────┐   binlog    ┌──────────┐   CDC events   ┌───────┐
│  MySQL  │ ──────────► │ Debezium │ ─────────────► │ Kafka │
└─────────┘             └──────────┘                └───┬───┘
                                                        │
                                    Spark Streaming (Scala/Python)
                                                        │
                                  ┌─────────────────────┴──────────────┐
                                  ▼                                    ▼
                           ┌──────────┐                          ┌──────────┐
                           │ MongoDB  │                          │  Redis   │
                           │ (query)  │                          │ (cache)  │
                           └──────────┘                          └──────────┘
                                  │                                    │
                                  └───────────┬────────────────────────┘
                                              │
                                    ┌─────────▼──────────┐
                                    │ metrics_exporter   │ :8000
                                    └─────────┬──────────┘
                                              │ scrape
                                    ┌─────────▼──────────┐
                                    │  Prometheus (9090) │
                                    └─────────┬──────────┘
                                              │
                                    ┌─────────▼──────────┐
                                    │   Grafana (3000)   │
                                    └────────────────────┘
```

---

## Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|------------|-----------|---------|
| OS | Linux (Ubuntu 22.04+) | Đã test trên Ubuntu |
| RAM | ≥ 16GB (khuyến nghị 32GB) | Spark cluster cần nhiều RAM |
| Disk | ≥ 10GB trống | Cho Docker volumes |
| Docker | 24.0+ | cùng `docker compose` v2 |
| Python | 3.10+ | cho `metrics_exporter.py` |
| sbt | 1.9+ | Chỉ cần nếu rebuild JAR Scala |

---

## Cài đặt (3 bước)

### 1. Clone repo

```bash
git clone <your-repo-url>
cd cdc-pipeline
```

### 2. Cài Python dependencies

```bash
pip3 install --break-system-packages pymysql pymongo redis kafka-python prometheus-client
```

### 3. Khởi động pipeline

```bash
bash start.sh
```

Chờ 2-3 phút. Khi thấy dòng `✓ Pipeline đã sẵn sàng!` là xong.

---

## Sử dụng

### Mở dashboards

| URL | Công cụ | Tài khoản |
|-----|---------|-----------|
| http://localhost:3000 | Grafana (dashboard chính) | admin / admin |
| http://localhost:8080 | Spark Master UI | — |
| http://localhost:9090 | Prometheus | — |
| http://localhost:8083 | Debezium Connect API | — |
| http://localhost:8000/metrics | Metrics raw | — |

### Scripts

```bash
# Khởi động (Scala mode — mặc định, nhanh nhất)
bash start.sh

# Khởi động bằng Python (fallback nếu JAR có vấn đề)
bash start.sh --python

# Dừng (giữ data)
bash stop.sh

# Dừng + xóa toàn bộ data
bash stop.sh -v
```

### Demo + Benchmark

```bash
# Demo loop INSERT/UPDATE/DELETE
bash demo.sh demo

# Benchmark TPS (50 records)
bash demo.sh tps

# Benchmark sustained (500 records, 10 batches)
bash demo.sh sustained

# Kiểm tra trạng thái
bash demo.sh status
```

### Scalability benchmark

```bash
# Đo throughput với nhiều config partition/trigger
bash benchmark/benchmark_scaling.sh --quick   # Nhanh
bash benchmark/benchmark_scaling.sh           # Full (15-20 phút)
```

---

## Rebuild Scala JAR

Nếu bạn sửa `jobs/scala/cdc_redis_consumer.scala`:

```bash
cd jobs/scala
sbt clean package

# Copy JAR mới đè JAR cũ
cp target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar ../cdc-mysql-to-mongodb-redis_2.12-1.0.jar

# Restart
cd ../..
bash stop.sh && bash start.sh
```

---

## Cấu trúc project

```
cdc-pipeline/
├── start.sh, stop.sh       # Scripts chính
├── demo.sh                 # Demo + benchmark đơn giản
├── README.md
├── .gitignore
├── .env.example            # Template credentials
│
├── pipeline/
│   └── docker-compose.yml  # Định nghĩa containers
│
├── demo/
│   ├── connector.json      # Debezium connector config
│   ├── init.sql            # MySQL schema
│   └── demodata.sql
│
├── jobs/
│   ├── cdc-*.jar           # Scala JAR pre-built
│   ├── python/
│   │   └── cdc_pipeline.py
│   └── scala/
│       ├── cdc_redis_consumer.scala
│       └── build.sbt
│
├── benchmark/
│   ├── benchmark_scaling.sh
│   ├── tps_benchmark.py
│   └── results/            # Kết quả (gitignored)
│
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── provisioning/
│       └── dashboards/
│
├── spark/
│   └── Dockerfile          # Custom Spark image
│
├── metrics_exporter.py     # Prometheus exporter
└── docs/
    ├── DEMO_GUIDE.md
    ├── VM_SETUP.md         # Deploy lên VM thuê
    └── SPARK_SETUP.md
```

---

## Chạy trên VM thuê (AWS/GCP/Azure/DigitalOcean)

Xem [`docs/VM_SETUP.md`](docs/VM_SETUP.md) để biết chi tiết scale benchmark trên cloud.

---

## Troubleshooting

### Kafka bị `InconsistentClusterIdException`

Script `start.sh` đã có auto-fix — nếu detect lỗi sẽ tự xóa volume Kafka + Zookeeper rồi khởi động lại.

Nếu muốn fix thủ công:
```bash
cd pipeline
docker compose down
docker volume rm pipeline_kafka_data pipeline_zookeeper_data
docker compose up -d
```

### Spark job không ghi được MongoDB

Kiểm tra URI trong Scala/Python — phải dùng hostname Docker (`cdc-mongodb`), không phải `localhost`.

### Grafana "No data"

Script tự patch datasource UID mỗi lần start. Nếu vẫn lỗi:
```bash
docker compose restart grafana
```

### Port đã bị chiếm

Các port: 3000, 3306, 6379, 7077, 8080-8085, 8083, 9090, 9092, 27017.

```bash
# Tìm process chiếm port
sudo lsof -i :<port>
```

---

## Tech stack

- **MySQL 8.0** — Source DB
- **Debezium 2.5** — CDC connector
- **Kafka 7.5 (Confluent)** — Message broker
- **Spark 3.5.0** — Streaming processor (Scala 2.12 / PySpark)
- **MongoDB 7.0** — Query layer
- **Redis 7** — Cache/serving layer
- **Prometheus + Grafana** — Monitoring
