# SETUP — CDC Data Pipeline

**Yêu cầu duy nhất: Docker Desktop** — không cần cài Python, Java, Spark hay bất kỳ thứ gì khác trên máy host.

---

## Yêu cầu hệ thống

| | Tối thiểu |
|---|---|
| **Docker Desktop** | 24.0+ (kèm Compose v2) |
| **RAM** | 12 GB trống cho Docker |
| **Disk** | 10 GB (images + volumes) |
| **OS** | Linux, macOS, Windows 10/11 |

> **Windows:** Chạy tất cả lệnh `.sh` trong **Git Bash** (có sẵn khi cài [Git for Windows](https://git-scm.com/download/win)).
> Không dùng PowerShell hay CMD — `curl` trong PowerShell là alias khác, sẽ lỗi.

---

## Cài đặt và chạy (3 bước)

```bash
# 1. Clone repo
git clone <your-repo-url>
cd cdc-data-pipeline

# 2. Pull Docker images (lần đầu ~5 phút)
docker compose pull

# 3. Khởi động pipeline
bash start.sh
```

Chờ **3–5 phút**. Khi thấy `✓ Pipeline đã sẵn sàng!` là xong.

---

## Dashboards

| URL | Mô tả | Tài khoản |
|---|---|---|
| http://localhost:3000 | **Grafana** — dashboard chính | `admin / admin` |
| http://localhost:8080 | Spark Master UI | — |
| http://localhost:9090 | Prometheus | — |
| http://localhost:8083 | Debezium Connect API | — |
| http://localhost:8000/metrics | Metrics Prometheus raw | — |
| http://localhost:8888 | **Live Demo Dashboard** | — |

---

## Lệnh thường dùng

```bash
# Khởi động (tự động detect hardware — Scala JAR mặc định)
bash start.sh

# Detect hardware và xem profile gợi ý
bash start.sh --detect

# Chọn profile thủ công
bash start.sh --profile=laptop     # laptop/PC ≥12GB RAM
bash start.sh --profile=server     # workstation ≥32GB RAM
bash start.sh --profile=vm         # cloud VM

# Dùng PySpark thay vì Scala (chậm hơn, fallback)
bash start.sh --python

# Dừng pipeline, giữ data
bash stop.sh

# Dừng pipeline + xóa toàn bộ data (volumes)
bash stop.sh -v

# Kiểm tra pipeline (43 checks)
bash scripts/test_smoke.sh

# Benchmark E2E (quick ~3 phút)
bash scripts/run_bench.sh

# Benchmark đầy đủ (~10 phút, dùng cho báo cáo)
bash scripts/run_bench.sh full

# So sánh các lần chạy benchmark
python benchmark/compare_runs.py -n 5
```

---

## Live Demo Dashboard

Dashboard tương tác để demo cho hội đồng.

```bash
# Cài dependencies demo server (1 lần duy nhất)
pip install -r demo/requirements.txt

# Chạy demo server
bash demo/run_demo.sh      # Linux / macOS / Git Bash
# hoặc
demo\run_demo.bat           # Windows CMD / PowerShell

# Mở trình duyệt: http://localhost:8888
```

Tính năng: bấm **▶ START DEMO** → chọn rate → xem MySQL / MongoDB / Redis / Kafka cập nhật real-time mỗi 2 giây.

> **Chạy trên máy khác (không phải localhost):**
> ```bash
> cp demo/.env.example demo/.env
> # Sửa 127.0.0.1 → IP của máy chạy pipeline
> ```

---

## Hardware Profiles

Pipeline tự động chọn profile. Để xem gợi ý cho máy hiện tại:

```bash
bash start.sh --detect
```

| Profile | RAM máy | Spark |
|---|---|---|
| `laptop` | 12–16 GB | 3 workers × 2g |
| `server` | 32 GB+ | 6 workers × 4g |
| `vm` | cloud VM | 6 workers × 4g |

Chỉnh thủ công bằng cách sửa `config/.env.laptop` / `config/.env.server` / `config/.env.vm` rồi chạy lại `bash start.sh`.

---

## Rebuild Scala JAR (chỉ khi sửa source)

Chỉ cần làm nếu chỉnh sửa `jobs/scala/cdc_redis_consumer.scala`.  
Yêu cầu: `sbt` + Java 11+ trên host.

```bash
cd jobs/scala
sbt clean package
cp target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar ../
cd ../..
bash stop.sh && bash start.sh
```

---

## Troubleshooting

### Kafka `InconsistentClusterIdException`
`start.sh` tự detect và fix. Nếu vẫn lỗi sau khi start:
```bash
docker compose down
docker volume rm pipeline_kafka_data pipeline_zookeeper_data
docker compose up -d
```

### Grafana "No data" / datasource lỗi
```bash
docker compose restart grafana
```

### Spark download packages chậm (~3–5 phút)
Bình thường ở lần đầu — Spark tải dependencies vào ivy2 cache. Từ lần khởi động sau sẽ nhanh hơn.

### Port bị chiếm
Ports dùng: `2181, 3000, 3306, 6379, 7077, 8000, 8080–8085, 8083, 9090, 9092, 27017`

```bash
# Linux / macOS
sudo lsof -i :<port>

# Windows (PowerShell)
netstat -ano | findstr :<port>
```

### Demo server lỗi kết nối
```bash
pip install -r demo/requirements.txt
bash demo/run_demo.sh
```

### Báo cáo trạng thái hiển thị `?` (Windows)
`start.sh` cần `python` hoặc `python3` trong PATH của Git Bash:
```bash
which python3 || which python
```

---

## Kiến trúc

```
MySQL  →  Debezium  →  Kafka  →  Spark Structured Streaming
                                   ├─→  MongoDB  (lưu trữ, truy vấn)
                                   └─→  Redis    (cache tốc độ cao)
                                              │
                                    metrics_exporter :8000
                                              │ scrape/5s
                                    Prometheus :9090
                                              │
                                    Grafana   :3000
```

Flow chi tiết:
- **MySQL binlog** → Debezium đọc không query trực tiếp → zero overhead
- **Kafka** giữ events bền vững — Spark tạm dừng không mất data
- **Spark** xử lý micro-batch mỗi 5 giây → ghi song song MongoDB + Redis
- **Prometheus** scrape metrics mỗi 5 giây từ custom exporter

---

## Tech stack

| Thành phần | Version | Vai trò |
|---|---|---|
| MySQL | 8.0 | Source database (CDC qua binlog) |
| Debezium | 2.5 | CDC connector |
| Kafka (Confluent) | 7.5.0 | Message broker |
| Spark | 3.5.0 | Structured Streaming processor |
| MongoDB | 7.0 | Sink — lưu trữ |
| Redis | 7 | Sink — cache |
| Prometheus | latest | Metrics scraper |
| Grafana | latest | Dashboard |
