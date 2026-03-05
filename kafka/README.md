# Kafka — Common Errors & Fix

Pipeline: `MySQL → Debezium → Kafka → Spark → Redis → MongoDB`

---

## 1. Kafka container bị Exited

**Triệu chứng:**
```
cdc-kafka     Exited
cdc-debezium  Exited
```

**Nguyên nhân:** Volume cũ bị corrupt, Kafka không đọc được cluster state.

**Fix:**
```bash
docker compose down -v
docker compose up -d
```

---

## 2. Debezium API không connect được

**Triệu chứng:**
```
curl: (7) Failed to connect to localhost port 8083
```

**Nguyên nhân:** Debezium container chưa ready — thường cần 20–30 giây sau khi `docker compose up`.

**Fix:**
```bash
# Kiểm tra container có đang chạy không
docker ps | grep debezium

# Nếu chưa chạy
docker compose restart debezium
```

---

## 3. Connector trả về 404

**Triệu chứng:**
```json
{ "error_code": 404, "message": "No status found for connector mysql-connector" }
```

**Nguyên nhân:** Connector chưa được đăng ký, hoặc bị mất sau khi restart container với `-v`.

**Fix:**
```bash
cd demo
bash register-connector.sh

# Verify
curl http://localhost:8083/connectors
# Kết quả: ["mysql-connector"]
```

---

## 4. Kafka topic command thất bại

**Triệu chứng:**
```
Error: container cdc-kafka is not running
```

**Nguyên nhân:** Kafka container crash, thường do Zookeeper chưa sẵn sàng khi Kafka khởi động.

**Fix:**
```bash
docker logs cdc-kafka | tail -20
docker compose restart zookeeper
docker compose restart kafka
```

---

## 5. Topic CDC không xuất hiện sau khi đăng ký connector

**Triệu chứng:** `kafka-topics --list` không thấy `mysql.inventory.customers`.

**Nguyên nhân:** Connector đăng ký thành công nhưng task chưa RUNNING, thường do MySQL binlog chưa bật.

**Fix:**
```bash
# Kiểm tra connector task status
curl -s http://localhost:8083/connectors/mysql-connector/status | python3 -m json.tool

# Kiểm tra binlog MySQL
docker exec cdc-mysql mysql -uroot -proot \
  -e "SHOW VARIABLES LIKE 'log_bin'; SHOW VARIABLES LIKE 'binlog_format';"
# log_bin phải là ON, binlog_format phải là ROW
```

---

## 6. Volume cũ bị chèn — Container không start sau khi đã `down -v`

**Triệu chứng:**
```bash
docker volume ls
# Thấy hàng loạt volume không tên (hash) lẫn volume cũ của pipeline:
# local  3cc16088f470...
# local  pipeline_mysql_data
# local  pipeline_mongodb_data
```
Container vẫn bị lỗi dù đã chạy `docker compose down -v`.

**Nguyên nhân:** `docker compose down -v` chỉ xóa volume được khai báo trong `docker-compose.yml`. Các volume anonymous (tên là hash) từ lần chạy trước vẫn còn tồn tại và có thể bị mount nhầm.

**Fix:**
```bash
# Xóa toàn bộ volume không còn được dùng bởi container nào
docker volume prune

# Nếu vẫn còn volume cụ thể của pipeline, xóa thủ công
docker volume rm pipeline_mysql_data pipeline_mongodb_data \
  pipeline_spark_data pipeline_spark_master_data \
  pipeline_spark_worker1_data pipeline_spark_worker2_data

# Sau đó start lại
docker compose up -d
```

> **Lưu ý:** `docker volume prune` xóa tất cả volume không gắn với container nào — an toàn khi không có project Docker khác đang dùng.

---

## 7. Port đã bị chiếm

**Triệu chứng:** Container không start, log báo `port already in use`.

**Fix:**
```bash
# Tìm process đang dùng port (ví dụ 9092)
sudo lsof -i :9092

# Kill process đó hoặc đổi port trong docker-compose.yml
```

---

## Bảng Tóm Tắt

| # | Triệu chứng | Nguyên nhân | Fix nhanh |
|---|---|---|---|
| 1 | Kafka/Debezium Exited | Volume corrupt | `docker compose down -v && up -d` |
| 2 | Port 8083 refused | Debezium chưa ready | Chờ 30s hoặc restart debezium |
| 3 | Connector 404 | Chưa đăng ký | Chạy lại `register-connector.sh` |
| 4 | Topic command thất bại | Kafka container crash | Restart zookeeper → kafka |
| 5 | Topic CDC không có | Connector task không RUNNING | Kiểm tra binlog MySQL |
| 6 | Volume cũ bị chèn | Volume anonymous còn sót lại | `docker volume prune` |
| 7 | Port in use | Conflict với process khác | Kill process hoặc đổi port |