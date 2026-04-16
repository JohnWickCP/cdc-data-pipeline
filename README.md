# CDC Pipeline — Hướng Dẫn Vận Hành

Pipeline tự động đồng bộ dữ liệu real-time từ MySQL → Kafka → Spark → MongoDB + Redis.

---

## Yêu cầu

- Docker + Docker Compose đã cài
- Python 3 với các thư viện: `pymysql pymongo redis kafka-python prometheus-client`

Cài thư viện (chỉ cần làm 1 lần):
```bash
pip3 install pymysql pymongo redis kafka-python prometheus-client --break-system-packages
```

---

## Khởi động (mọi tình huống)

Chỉ cần 2 lệnh — script tự xử lý mọi thứ còn lại:

```bash
cd ~/cdc-pipeline/pipeline && docker compose up -d
cd ~/cdc-pipeline && bash reset_pipeline.sh
```

Chờ khoảng **2-3 phút** là xong. Script tự động:
- Detect và fix Kafka Cluster ID conflict (nếu có)
- Khởi động container còn thiếu
- Xóa state cũ, register Debezium connector
- Khởi động Spark job + metrics exporter
- Chờ MongoDB sync và báo kết quả

**Kết quả mong đợi cuối script:**
```
  MySQL      customers : 3
  MongoDB    customers : 3
  Redis      keys      : 11
  ✅ Pipeline hoạt động bình thường — sẵn sàng demo!
```

---

## Sau khi khởi động xong

Mở trình duyệt vào các địa chỉ sau:

| Địa chỉ | Dùng để làm gì |
|---------|----------------|
| http://localhost:3000 | Grafana dashboard (user: admin / pass: admin) |
| http://localhost:8080 | Spark UI — xem job đang chạy |
| http://localhost:8000/metrics | Metrics raw của pipeline |

---

## Chạy bài test

```bash
cd ~/cdc-pipeline && python3 test_pipeline.py
```

Kết quả sẽ in ra màn hình và lưu vào file `test_report.txt`.

---

## Quy trình chuẩn trước khi demo cho thầy

```
1. Bật máy, mở terminal
2. cd ~/cdc-pipeline/pipeline && docker compose up -d
3. cd ~/cdc-pipeline && bash reset_pipeline.sh
4. Chờ script chạy xong (~2-3 phút)
5. Kiểm tra: MySQL customers = MongoDB customers = 3
6. Mở http://localhost:3000 → đăng nhập Grafana (admin/admin)
7. Chạy python3 test_pipeline.py để kiểm tra
8. Sẵn sàng demo!
```

> **Lưu ý:** Không cần chạy `docker compose up -d` nếu containers đã đang chạy.
> `reset_pipeline.sh` tự detect và khởi động container còn thiếu.

---

## Nếu gặp sự cố

### Pipeline không chạy / Grafana không có data

```bash
cd ~/cdc-pipeline && bash reset_pipeline.sh
```

Chạy lại reset là giải pháp cho hầu hết mọi vấn đề — script tự xử lý các lỗi phổ biến.

---

### Kafka bị lỗi Cluster ID conflict

Xảy ra khi chạy `docker compose down -v` rồi `up` lại. `reset_pipeline.sh` tự detect và fix lỗi này. Nếu muốn fix thủ công:

```bash
cd ~/cdc-pipeline/pipeline
docker compose down -v
docker compose up -d
# Chờ 60s rồi chạy reset
cd ~/cdc-pipeline && bash reset_pipeline.sh
```

---

### Spark không sync data vào MongoDB

```bash
# Xem log Spark để biết lỗi cụ thể
docker exec cdc-spark-master tail -50 /tmp/spark_metrics.log
```

Nếu thấy lỗi `UnknownTopicOrPartitionException` → checkpoint cũ bị stale, chạy lại `reset_pipeline.sh` là xong.

---

### Xem log để debug

```bash
# Spark log
docker exec cdc-spark-master tail -f /tmp/spark_metrics.log

# Metrics exporter log
tail -f ~/cdc-pipeline/metrics_exporter.log

# Debezium log
docker logs cdc-debezium --tail 30
```

---

### Kiểm tra nhanh pipeline có hoạt động không

```bash
# MySQL
docker exec cdc-mysql mysql -uroot -proot -e "SELECT COUNT(*) FROM inventory.customers;"

# MongoDB
docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').customers.countDocuments()"

# Redis
docker exec cdc-redis redis-cli keys "*"

# Debezium connector
curl -s http://localhost:8083/connectors/mysql-inventory-connector/status | python3 -m json.tool
```

---

### Tắt toàn bộ hệ thống

```bash
# Giữ volumes (khuyến nghị — bật lại nhanh hơn)
cd ~/cdc-pipeline/pipeline && docker compose down

# Xóa sạch volumes (dùng khi muốn reset hoàn toàn)
cd ~/cdc-pipeline/pipeline && docker compose down -v
```

---

## Cấu trúc project

```
cdc-pipeline/
├── demo/                  # Dữ liệu demo và connector config
│   ├── connector.json     # Debezium connector config (snapshot.mode: initial)
│   ├── demodata.sql       # Dữ liệu demo gốc (3 customers, 4 orders)
│   └── init.sql           # Schema khởi tạo MySQL
├── docs/                  # Tài liệu và kịch bản demo
├── jobs/scala/            # Code Spark (Scala)
├── monitoring/            # Config Prometheus
├── pipeline/              # Docker Compose
├── screenshots/           # Ảnh chụp màn hình
├── spark/                 # Dockerfile cho Spark
├── reset_pipeline.sh      # Script reset môi trường ⭐
├── test_pipeline.py       # Script chạy bài test ⭐
└── metrics_exporter.py    # Script export metrics cho Grafana ⭐
```

---

## Kiến trúc hệ thống

```
MySQL (OLTP)
   ↓ binlog
Debezium (CDC)
   ↓ events
Kafka (message queue)
   ↓ stream
Spark Structured Streaming
   ↓              ↓
MongoDB          Redis
(full records)   (metrics)
   ↓
Prometheus → Grafana (dashboard)
```

---

## Kết quả test (tham khảo)

| Chỉ số | Kết quả | Mục tiêu |
|--------|---------|----------|
| Accuracy | 8/8 passed | 100% |
| End-to-end latency | ~4.5s | < 10s |
| Throughput | ~1.6 records/s | - |
| Fault tolerance | Phục hồi sau 4s | - |