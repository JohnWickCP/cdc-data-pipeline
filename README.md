# CDC Pipeline — MySQL → Kafka → Spark → MongoDB + Redis

Hệ thống **Change Data Capture (CDC)** đồng bộ dữ liệu real-time từ MySQL sang MongoDB và Redis thông qua Debezium + Kafka + Spark Structured Streaming, có monitoring bằng Prometheus + Grafana.

![Pipeline đang chạy](screenshots/01-full-pipeline-running.png)

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

## Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| **Docker Desktop** | 24.0+ | Bắt buộc, kèm `docker compose` v2 |
| **Git Bash** | bất kỳ | Windows cần Git Bash để chạy `.sh` script |
| **RAM** | ≥ 12GB | Spark cluster + tất cả containers |
| **Disk** | ≥ 10GB | Docker images + volumes |
| **Python** | 3.9+ | **Không cần cài trên host** — chạy trong Docker |
| **sbt** | 1.9+ | Chỉ cần nếu bạn muốn **tự build lại** JAR Scala |

> **Lưu ý Windows:** Chạy tất cả lệnh `.sh` trong **Git Bash**, không phải PowerShell hay CMD.

---

## Cài đặt và chạy (3 bước)

### 1. Clone repo

```bash
git clone <your-repo-url>
cd cdc-data-pipeline
```

### 2. Pull Docker images (lần đầu ~5 phút)

```bash
cd pipeline
docker compose pull
cd ..
```

> Nếu bỏ qua bước này, `start.sh` vẫn tự pull, nhưng chậm hơn vì chạy song song với build image Spark.

### 3. Khởi động pipeline

```bash
bash start.sh
```

Chờ khoảng **3–5 phút**. Script sẽ tự động:
- Khởi động 12 containers
- Khởi tạo MySQL schema + dữ liệu mẫu
- Đăng ký Debezium connector
- Submit Spark Structured Streaming job
- Báo cáo trạng thái khi xong

Khi thấy dòng `✓ Pipeline đã sẵn sàng!` → pipeline đang chạy.

---

## Truy cập dashboards

| URL | Công cụ | Tài khoản |
|---|---|---|
| http://localhost:3000 | **Grafana** — dashboard chính | `admin / admin` |
| http://localhost:8080 | Spark Master UI | — |
| http://localhost:9090 | Prometheus | — |
| http://localhost:8083 | Debezium Connect API | — |
| http://localhost:8000/metrics | Metrics raw (Prometheus format) | — |

---

## Scripts

```bash
# Khởi động (Scala JAR — mặc định, hiệu năng cao nhất)
bash start.sh

# Khởi động bằng PySpark (fallback nếu JAR có vấn đề)
bash start.sh --python

# Dừng pipeline, giữ nguyên data
bash stop.sh

# Dừng pipeline + xóa toàn bộ data (volumes)
bash stop.sh -v

# Chạy benchmark E2E TPS (quick mode ~3 phút)
bash run_bench.sh

# Chạy benchmark full (~10 phút, dùng cho báo cáo)
bash run_bench.sh full
```

---

## Cấu trúc project

```
cdc-data-pipeline/
│
├── start.sh                    # Khởi động toàn bộ pipeline (script chính)
├── stop.sh                     # Dừng pipeline
├── run_bench.sh                # Chạy benchmark nhanh
├── demo.sh                     # Script demo đầy đủ
├── README.md                   # File này
├── KNOWN_ISSUES.md             # Danh sách vấn đề đã biết + trạng thái
├── .gitignore
│
├── pipeline/
│   └── docker-compose.yml      # Định nghĩa 12 containers
│
├── demo/
│   ├── connector.json          # Cấu hình Debezium MySQL connector
│   ├── init.sql                # MySQL schema khởi tạo
│   └── demodata.sql            # Dữ liệu mẫu
│
├── jobs/
│   ├── cdc-mysql-to-mongodb-redis_2.12-1.0.jar   # Scala JAR đã build sẵn
│   ├── python/
│   │   └── cdc_pipeline.py     # PySpark job (fallback)
│   └── scala/
│       ├── cdc_redis_consumer.scala
│       ├── build.sbt
│       └── README-scala.md
│
├── benchmark/
│   ├── run_benchmark_v4.py     # Script đo E2E TPS (chạy trong Docker)
│   ├── tps_benchmark.py        # Script đo TPS đơn giản
│   ├── benchmark_scaling.sh    # Test scale theo partition/worker
│   └── results/                # Kết quả benchmark (gitignored, trừ .gitkeep)
│
├── monitoring/
│   ├── prometheus.yml          # Cấu hình Prometheus scrape
│   ├── exporter/
│   │   ├── metrics_exporter.py # Thu thập metrics từ MySQL/Mongo/Redis/Kafka
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── .env                # Biến môi trường (xem .env để biết các key)
│   └── grafana/
│       ├── dashboards/         # JSON dashboard
│       └── provisioning/       # Auto-provision datasource + dashboard
│
├── spark/
│   └── Dockerfile              # Custom Spark 3.5.0 image với JAR dependencies
│
└── docs/
    ├── DEMO_SCRIPT.md          # Kịch bản demo 8 phút cho thầy hướng dẫn
    ├── SPARK_SETUP.md          # Chi tiết Spark job và cách hoạt động
    └── VM_SETUP.md             # Deploy lên cloud VM để benchmark scale
```

---

## Rebuild Scala JAR

Chỉ cần làm nếu bạn chỉnh sửa `jobs/scala/cdc_redis_consumer.scala`:

```bash
cd jobs/scala
sbt clean package

# Copy JAR mới vào đúng vị trí
cp target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar ../

# Restart pipeline
cd ../..
bash stop.sh && bash start.sh
```

> Yêu cầu: `sbt` và `Java 11+` cài trên host.

---

## Troubleshooting

### Kafka bị `InconsistentClusterIdException`

`start.sh` tự detect và fix: xóa volume Kafka + Zookeeper rồi khởi động lại. Không cần làm gì.

Nếu muốn fix thủ công:
```bash
cd pipeline
docker compose down
docker volume rm pipeline_kafka_data pipeline_zookeeper_data
docker compose up -d
```

### Spark job không ghi được MongoDB / Redis

Kiểm tra hostname — phải dùng tên container Docker (`cdc-mongodb`, `cdc-redis`), không phải `localhost`.

### Grafana "No data" / datasource lỗi

```bash
cd pipeline && docker compose restart grafana
```

### Port đã bị chiếm

Các port dùng: `2181, 3000, 3306, 6379, 7077, 8000, 8080–8085, 8083, 9090, 9092, 27017`

```bash
# Linux/Mac
sudo lsof -i :<port>

# Windows (PowerShell)
netstat -ano | findstr :<port>
```

### Báo cáo trạng thái hiển thị `?` (Windows)

`start.sh` cần `python` hoặc `python3` trong PATH của Git Bash. Đã được fix trong phiên bản hiện tại bằng cách tự detect interpreter. Nếu vẫn bị, chạy:
```bash
which python3 || which python
```

---

## Tech stack

| Thành phần | Version | Vai trò |
|---|---|---|
| MySQL | 8.0 | Source database (CDC qua binlog) |
| Debezium | 2.5 | CDC connector (đọc binlog → Kafka) |
| Kafka (Confluent) | 7.5.0 | Message broker |
| Spark | 3.5.0 | Structured Streaming processor |
| MongoDB | 7.0 | Sink — lưu trữ và truy vấn |
| Redis | 7 | Sink — cache tốc độ cao |
| Prometheus | latest | Scrape metrics |
| Grafana | latest | Dashboard |

---

## Kết quả benchmark (laptop i5-11400H, 12 cores, Docker local)

| Mức inject | E2E TPS thật | Lag cuối |
|---|---|---|
| 100 TPS | 79.8 rec/s | 0 |
| 200 TPS | 153.3 rec/s | 0 |
| 500 TPS | 405.6 rec/s | 0 |
| Ổn định 324 TPS × 30s | 272.5 rec/s | 0 |

> **E2E TPS** = records vào MongoDB / (thời gian inject + drain). Xem `KNOWN_ISSUES.md` để biết giới hạn của phép đo này.
