# Kịch Bản Demo — CDC Pipeline
**Thời gian:** ~8 phút | **Đối tượng:** Giảng viên hướng dẫn

---

## Chuẩn bị trước khi demo (làm trước 5 phút)

```bash
# 1. Đảm bảo tất cả container đang chạy
docker ps --format "table {{.Names}}\t{{.Status}}"

# 2. Đảm bảo connector đang RUNNING
curl -s http://localhost:8083/connectors/mysql-connector/status | python3 -m json.tool

# 3. Reset data về trạng thái sạch
docker exec -i cdc-mysql mysql -uroot -proot < demo/demo-data.sql

# 4. Mở sẵn Spark (terminal riêng — để chạy ngay khi cần)
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala

# 5. Mở sẵn 3 terminal:
#    Terminal A — chạy lệnh MySQL
#    Terminal B — Spark đang chạy (bước 4)
#    Terminal C — verify Redis/MongoDB
```

---

## Phần 1 — Giới thiệu kiến trúc (1 phút)

**Nói:**
> "Đây là pipeline CDC — Change Data Capture. Ý tưởng là: mỗi khi có thay đổi dữ liệu trong MySQL, hệ thống tự động bắt và stream sự thay đổi đó đến các hệ thống khác theo thời gian thực, không cần polling.
> Pipeline gồm 5 thành phần: MySQL là nguồn dữ liệu, Debezium đọc binlog của MySQL và đẩy event vào Kafka, Spark Structured Streaming consume từ Kafka rồi ghi đồng thời vào Redis và MongoDB."

**Show:** Chạy lệnh này để minh họa tất cả đang sống:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**Nói:** *"9 container, tất cả Up — đây là toàn bộ hệ thống."*

---

## Phần 2 — CDC: MySQL → Kafka (2 phút)

**Nói:**
> "Trước tiên mình xem dữ liệu ban đầu trong MySQL."

**Chạy — Terminal A:**
```bash
docker exec cdc-mysql mysql -uroot -proot \
  -e "USE inventory; SELECT * FROM customers;"
```

**Nói:** *"Có 3 khách hàng. Bây giờ mình INSERT thêm 1 người — và xem Debezium có bắt được event không."*

**Chạy — Terminal A:**
```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
INSERT INTO customers (name, email) VALUES ('Pham Duc Duy', 'duy.pham@company.com');
"
```

**Chạy — Terminal A (ngay sau):**
```bash
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mysql.inventory.customers \
  --max-messages 1 2>/dev/null | python3 -c "
import sys, json
data = json.loads(sys.stdin.read())
p = data['payload']
print('op   :', p['op'])
print('after:', p['after'])
"
```

**Nói:** *"op = 'c' — create. Debezium đã capture sự kiện INSERT và đẩy vào Kafka topic ngay lập tức. Đây là nền tảng của CDC."*

---

## Phần 3 — Spark Stream → Redis + MongoDB (2 phút)

**Nói:**
> "Spark đang consume liên tục từ Kafka topic đó. Mình chuyển sang terminal Spark."

**Show — Terminal B** *(Spark đang chạy, có thể thấy các dòng Batch)*

**Nói:** *"Mỗi 5 giây Spark xử lý một batch. Mình INSERT thêm data và xem Spark phản ứng."*

**Chạy — Terminal A:**
```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
INSERT INTO customers (name, email) VALUES ('Hoang Thi Em', 'em.hoang@company.com');
INSERT INTO customers (name, email) VALUES ('Vo Van Phuc',  'phuc.vo@company.com');
"
```

**Chờ ~5 giây** → Spark terminal sẽ in:
```
=== Batch N (2 records) ===
[MongoDB] UPSERT id=5 name=Hoang Thi Em ...
[Redis] SET customer:5
[MongoDB] UPSERT id=6 name=Vo Van Phuc ...
[Redis] SET customer:6
```

**Verify — Terminal C:**
```bash
# Redis
docker exec cdc-redis redis-cli GET customer:5

# MongoDB
docker exec cdc-mongodb mongosh \
  --eval "db.getSiblingDB('inventory').customers.find().pretty()" --quiet
```

**Nói:** *"Data đã có mặt ở cả Redis lẫn MongoDB. Latency từ lúc INSERT MySQL đến lúc thấy ở Redis chưa đến 10 giây."*

---

## Phần 4 — Idempotent Upsert: Không Duplicate (1.5 phút)

**Nói:**
> "Một vấn đề thực tế trong streaming là duplicate — cùng 1 event bị xử lý nhiều lần. Pipeline này giải quyết bằng idempotent upsert."

**Chạy — Terminal A:**
```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
UPDATE customers SET email='duy.new@company.com' WHERE id=4;
"
```

**Chờ ~5 giây**, rồi chạy lại UPDATE thêm 1 lần nữa:
```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
UPDATE customers SET email='duy.new@company.com' WHERE id=4;
"
```

**Verify — Terminal C:**
```bash
# Đếm documents MongoDB — phải vẫn là 6, không tăng
docker exec cdc-mongodb mongosh \
  --eval "db.getSiblingDB('inventory').customers.countDocuments()" --quiet

# Redis trả về email mới nhất, không bị ghi đè sai
docker exec cdc-redis redis-cli GET customer:4
```

**Nói:** *"UPDATE 2 lần — MongoDB vẫn chỉ có 6 documents, không tạo thêm. Redis chỉ giữ giá trị mới nhất. Đây là idempotent upsert: dùng replaceOne với upsert=true cho MongoDB và SET overwrite cho Redis."*

---

## Phần 5 — Checkpoint: Không Mất Data Sau Restart (1.5 phút)

**Nói:**
> "Điểm cuối — fault tolerance. Nếu Spark bị dừng giữa chừng, khi restart nó có tiếp tục từ đúng chỗ không, hay xử lý lại từ đầu?"

**Chạy — Terminal B:** Nhấn `Ctrl+C` để dừng Spark.

**Chạy — Terminal A** *(INSERT trong lúc Spark không chạy)*:
```bash
docker exec cdc-mysql mysql -uroot -proot -e "
USE inventory;
INSERT INTO customers (name, email) VALUES ('Nguyen Offline', 'offline@company.com');
"
```

**Nói:** *"Spark đang tắt, nhưng Kafka vẫn đang giữ message này. Giờ restart Spark — không xóa checkpoint."*

**Chạy — Terminal B:**
```bash
docker exec -it cdc-spark-master /opt/spark/bin/spark-shell \
  --master local[2] \
  -i /opt/spark/work-dir/cdc_redis_consumer.scala
```

**Chờ ~10 giây** → Spark sẽ xử lý đúng record bị bỏ lỡ.

**Verify — Terminal C:**
```bash
docker exec cdc-mongodb mongosh \
  --eval "db.getSiblingDB('inventory').customers.find({name:'Nguyen Offline'}).pretty()" --quiet
```

**Nói:** *"Record 'Nguyen Offline' đã được xử lý sau khi restart — không mất, không duplicate. Đây là checkpoint của Spark Structured Streaming: nó lưu offset Kafka đã xử lý, khi restart sẽ tiếp tục từ đúng vị trí đó."*

---

## Kết Luận (30 giây)

**Nói:**
> "Tóm lại, tuần này mình đã hoàn thiện foundation của pipeline:
> - CDC hoạt động real-time qua Debezium + Kafka
> - Spark Structured Streaming xử lý và ghi đồng thời vào Redis và MongoDB
> - Idempotent upsert đảm bảo không có duplicate dù event đến nhiều lần
> - Checkpoint đảm bảo không mất data khi hệ thống restart
>
> Đây là nền tảng để các tuần tiếp theo mở rộng thêm transformation, monitoring, và tối ưu hiệu năng."

---

## Bảng Thời Gian

| Phần | Nội dung | Thời gian |
|---|---|---|
| 1 | Giới thiệu kiến trúc + show docker ps | ~1 phút |
| 2 | CDC: INSERT MySQL → Kafka event | ~2 phút |
| 3 | Spark stream → Redis + MongoDB | ~2 phút |
| 4 | Idempotent upsert | ~1.5 phút |
| 5 | Checkpoint sau restart | ~1.5 phút |
| Kết luận | | ~30 giây |
| **Tổng** | | **~8.5 phút** |
