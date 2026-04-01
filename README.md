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

## Khởi động lần đầu (hoặc sau khi tắt máy)

**Bước 1 — Khởi động toàn bộ hệ thống:**
```bash
cd ~/cdc-pipeline/pipeline
docker compose up -d
```
Chờ khoảng **40 giây** cho tất cả service khởi động.

**Bước 2 — Kiểm tra tất cả đang chạy:**
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Phải thấy 11 container, tất cả `Up` và `healthy`.

**Bước 3 — Reset và khởi động pipeline:**
```bash
cd ~/cdc-pipeline
bash reset_pipeline.sh
```
Script này tự động làm tất cả — chờ khoảng **1-2 phút** là xong.

---

## Sau khi reset_pipeline.sh chạy xong

Mở trình duyệt vào các địa chỉ sau:

| Địa chỉ | Dùng để làm gì |
|---------|----------------|
| http://localhost:3000 | Grafana dashboard (user: admin / pass: admin) |
| http://localhost:8080 | Spark UI — xem job đang chạy |
| http://localhost:8000/metrics | Metrics raw của pipeline |

---

## Chạy bài test

```bash
cd ~/cdc-pipeline
python3 test_pipeline.py
```

Kết quả sẽ in ra màn hình và lưu vào file `test_report.txt`.

---

## Quy trình chuẩn trước khi demo cho thầy

```
1. Bật máy, mở terminal
2. cd ~/cdc-pipeline/pipeline && docker compose up -d
3. Chờ 40 giây
4. cd ~/cdc-pipeline && bash reset_pipeline.sh
5. Chờ 1-2 phút
6. Kiểm tra: MySQL customers = MongoDB customers = 3
7. Mở http://localhost:3000 → đăng nhập Grafana
8. Chạy python3 test_pipeline.py để kiểm tra
9. Sẵn sàng demo!
```

---

## Nếu gặp sự cố

### Pipeline không chạy / Grafana không có data
```bash
cd ~/cdc-pipeline && bash reset_pipeline.sh
```
Chạy lại reset là giải pháp cho hầu hết mọi vấn đề.

---

### Debezium lỗi "db history topic is missing"
Xảy ra khi Kafka topics bị xóa nhưng Debezium vẫn còn nhớ state cũ.

**Cách fix:**
```bash
# Bước 1: Xóa connector cũ
curl -s -X DELETE http://localhost:8083/connectors/mysql-inventory-connector

# Bước 2: Restart Debezium
docker restart cdc-debezium
echo "Chờ 30s..."
sleep 30

# Bước 3: Register lại với recovery mode
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-inventory-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": "cdc-mysql",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "root",
      "database.server.id": "184054",
      "topic.prefix": "inventory",
      "database.include.list": "inventory",
      "table.include.list": "inventory.customers,inventory.orders",
      "schema.history.internal.kafka.bootstrap.servers": "cdc-kafka:29092",
      "schema.history.internal.kafka.topic": "schema-changes.inventory",
      "schema.history.internal.store.only.captured.tables.ddl": "true",
      "snapshot.mode": "schema_only_recovery",
      "include.schema.changes": "true",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'
```

Sau đó kiểm tra connector RUNNING chưa:
```bash
curl -s http://localhost:8083/connectors/mysql-inventory-connector/status \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tasks'][0]['state'])"
```

---

### Spark không sync data vào MongoDB
```bash
# Dừng Spark
docker exec cdc-spark-master bash -c "pkill -f CdcRedisConsumer 2>/dev/null; exit 0"
sleep 3

# Xóa checkpoint
docker exec cdc-spark-master bash -c "rm -rf /tmp/spark-checkpoint"

# Khởi động lại
docker exec -d cdc-spark-master /bin/bash -c \
  "/opt/spark/bin/spark-submit \
    --class CdcRedisConsumer \
    --master spark://cdc-spark-master:7077 \
    /opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar \
    > /tmp/spark_metrics.log 2>&1"
```

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
cd ~/cdc-pipeline/pipeline && docker compose down
```

---

## Cấu trúc project

```
cdc-pipeline/
├── demo/                  # Dữ liệu demo và connector config
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
| End-to-end latency | ~5s | < 10s |
| Throughput | ~1.6 records/s | - |
| Fault tolerance | Phục hồi sau 3s | - |