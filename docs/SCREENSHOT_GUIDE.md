# Hướng dẫn chụp ảnh — CDC Data Pipeline

Danh sách ảnh cần chụp, lệnh chuẩn bị, và tên file lưu vào `docs/screenshots/`.

**Yêu cầu trước khi chụp:** pipeline phải đang chạy đầy đủ (`bash start.sh`), smoke test 43/43 PASS.

---

## Ảnh cần chụp mới (chưa có)

### 07-grafana-dashboard.png

**Mô tả:** Dashboard Grafana đang hiển thị data real-time — panels Records Count, Throughput, Kafka Lag, Spark batch duration.

**Chuẩn bị:**
```bash
# Inject một ít data để dashboard có gì hiển thị
docker exec cdc-mysql mysql -uroot -proot inventory -e "
  INSERT INTO customers (id,name,email,phone) VALUES
    (9001,'Test A','testa@gmail.com','0900000001'),
    (9002,'Test B','testb@gmail.com','0900000002'),
    (9003,'Test C','testc@gmail.com','0900000003');
"
# Chờ 10-15 giây để Spark xử lý xong
```

**Chụp:**
1. Mở http://localhost:3000 (admin/admin)
2. Vào dashboard "CDC Pipeline Dashboard"
3. Đặt time range = Last 5 minutes
4. Chụp toàn màn hình khi panels hiển thị: Records Count, Throughput rates, Kafka Lag, Spark batch duration
5. Lưu: `docs/screenshots/07-grafana-dashboard.png`

---

### 10-benchmark-results.png

**Mô tả:** Bảng so sánh kết quả benchmark nhiều lần chạy trong terminal.

**Chuẩn bị:** Phải có ít nhất 2-3 lần chạy trong `benchmark/results/history.jsonl`.

```bash
# Nếu chưa có kết quả, chạy benchmark quick mode trước
bash scripts/run_bench.sh
```

**Chụp:**
```bash
python benchmark/compare_runs.py -n 5
```
1. Chạy lệnh trên trong terminal (Git Bash hoặc cmd)
2. Chụp toàn bộ output bảng kết quả
3. Lưu: `docs/screenshots/10-benchmark-results.png`

---

### 11-demo-dashboard.png

**Mô tả:** Demo dashboard đang chạy real-time — chart Chart.js đang vẽ, nút đang bấm, số liệu đang cập nhật.

**Chuẩn bị:**
```bash
pip install -r demo/requirements.txt   # Chỉ cần lần đầu
bash demo/run_demo.sh
```

**Chụp:**
1. Mở http://localhost:8888
2. Bấm nút **START DEMO**
3. Chờ 10-15 giây cho chart bắt đầu vẽ
4. Chụp khi chart đang có data và các số liệu (MySQL count, MongoDB count, Redis count) đang tăng
5. Lưu: `docs/screenshots/11-demo-dashboard.png`

---

## Ảnh đã có (kiểm tra lại chất lượng)

| File | Mô tả | Trạng thái |
|------|--------|------------|
| `01-full-pipeline-running.png` | 12 containers healthy (`docker ps`) | Có — kiểm tra còn đúng không |
| `02-mysql-initial-data.png` | Data trong MySQL (`SELECT * FROM customers`) | Có |
| `03-debezium-connector-running.png` | Debezium connector status = RUNNING | Có |
| `04-kafka-cdc-insert-event.png` | Kafka message JSON cho INSERT event | Có |
| `05-kafka-cdc-update-event.png` | Kafka message JSON cho UPDATE event | Có |
| `06-kafka-cdc-delete-event.png` | Kafka message JSON cho DELETE event | Có |
| `08-spark-master-ui.png` | Spark Master UI — app đang RUNNING | Có |
| `redis-cli-customer-data.png` | Redis CLI — `HGETALL customer:X` output | Có |

---

## Lệnh chụp nhanh từng ảnh (nếu cần làm lại)

### 01 — Docker containers healthy
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
# Chụp terminal output
```

### 02 — MySQL initial data
```bash
docker exec cdc-mysql mysql -uroot -proot inventory \
  -e "SELECT id, name, email, phone FROM customers LIMIT 10;" 2>/dev/null
```

### 03 — Debezium connector status
```bash
curl -s http://localhost:8083/connectors/mysql-inventory-connector/status | python -m json.tool
# Chụp phần "state": "RUNNING"
```

### 04/05/06 — Kafka CDC events
```bash
# Xem event INSERT (op=c), UPDATE (op=u), DELETE (op=d)
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inventory.inventory.customers \
  --from-beginning --max-messages 5 2>/dev/null
```

### 08 — Spark Master UI
```
Mở http://localhost:8080 → chụp phần "Running Applications"
```

### redis-cli — Customer data
```bash
docker exec cdc-redis redis-cli HGETALL customer:1
docker exec cdc-redis redis-cli GET customers:total
```
