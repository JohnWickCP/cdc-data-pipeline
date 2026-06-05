<!--
  Prompt cho session BOTTLENECK HUNTING — xóa sau khi dùng.
  Ước tính thời gian: 60–90 phút chạy thật + phân tích.
-->

# Session: Tìm Bottleneck Thật — CDC Pipeline

Đọc memory `session_handoff_fixes.md` + `chapter3_design_analysis.md` + `project_overview.md` trước.

---

## Quyền và phạm vi

**Bạn có toàn quyền bypass** — chạy thoải mái:
- `docker compose up/down/restart` bất kỳ service nào
- Xóa/tạo lại Kafka topics, thay đổi partition count
- Kill và restart Spark job
- Chạy benchmark bất kỳ bao lâu (kể cả 60 phút)
- Không cần xin phép trước mỗi bước

**Session này được phép chạy lâu** — đừng tóm tắt sớm, đừng lo về context limit.
Nếu context gần đầy, dùng `/compact` và tiếp tục.

---

## Bối cảnh — Những gì đã biết

### Kết quả baseline (tính đến 2026-06-05, commit `7809354`)

| Run | Partitions | Max E2E | Bottleneck | Ghi chú |
|-----|------------|---------|------------|---------|
| stress (2026-05-25) | 1 | 855.6 rec/s | ✅ Tìm được | Lag tích lũy > 1000 rec/s |
| full (2026-06-04) | 3 | **1567.8 rec/s** | ❌ Chưa tìm | Inject đến 2000, pipeline vẫn kịp |
| VM full (2026-05-27) | 12 | 1633.1 rec/s | ❌ Chưa tìm | Xeon E5-2690 |

**E2E Latency** (lần đầu đo 2026-06-04): P50=4914ms / P95=5430ms  
**Hardware laptop**: i5-11400H, 12 cores, 15.5GB RAM, 3 Spark workers × 4 cores  
**Pipeline**: MySQL → Debezium → Kafka → Spark (trigger 5s) → MongoDB + Redis

### Câu hỏi chưa trả lời

1. **Bottleneck thật ở đâu?** Debezium? Kafka? Spark? MongoDB? Redis?
2. **Partition count ảnh hưởng thế nào?** 1→3→6→12 → throughput tăng theo quy luật gì?
3. **Giới hạn thật của pipeline này trên laptop là bao nhiêu rec/s?**
4. **Throughput có suy giảm theo thời gian không?** (sau 10–30 phút liên tục)

---

## Mục tiêu session này

1. **Tìm bottleneck chính xác** — stage nào làm pipeline chậm nhất
2. **Đo throughput tại các partition count**: 1, 2, 3, 4, 6 partitions
3. **Xác định giới hẹn cứng** — inject rate tại đó Kafka lag không drain được
4. **Đo sustained 10 phút** — verify throughput ổn định hay suy giảm
5. **Ghi toàn bộ số liệu** vào `docs/BENCHMARK_RESULTS.md`

---

## Kế hoạch thực hiện

### Phase 0 — Khởi động pipeline sạch

```bash
bash stop.sh -v && bash start.sh
bash scripts/test_smoke.sh
# Phải 43/43 PASS trước khi bắt đầu
```

Nếu Spark crash `UnknownTopicOrPartitionException`:
- start.sh silent-fail ở bước đăng ký connector
- Fix: `curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @demo/config/connector.json`
- Chờ topics tạo, submit Spark thủ công:
  ```bash
  MSYS_NO_PATHCONV=1 docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
    --class CdcRedisConsumer --master spark://cdc-spark-master:7077 \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0" \
    /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar
  ```

---

### Phase 1 — Stress test baseline (3 partitions hiện tại)

**Mục tiêu:** Tìm giới hạn với cấu hình hiện tại (3 partitions).

```bash
# Chạy stress mode — inject 100→500→1000→2000→5000 rec/s
MSYS_NO_PATHCONV=1 docker exec cdc-metrics-exporter python -u /app/benchmark/run_benchmark_v4.py stress
```

Nếu benchmark script không có mode cao hơn 2000, sửa tạm thời:
```python
# Trong run_benchmark_v4.py, tìm STRESS_LEVELS và thêm:
STRESS_LEVELS = [100, 500, 1000, 2000, 3000, 5000]
```

**Chỉ số cần ghi:**
```
Partition count: 3
  100 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  500 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  1000 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  2000 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  3000 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  5000 rec/s → E2E: ___, Kafka lag: ___, Spark p50: ___
  
  Bottleneck bắt đầu tại: ___ rec/s
  Dấu hiệu: Kafka lag tích lũy / Spark batch >5s / ...
```

---

### Phase 2 — Partition sweep (tìm ảnh hưởng của partitions)

**Mục tiêu:** Đo max throughput tại mỗi partition count. Chạy full mode cho mỗi giá trị.

**Script thay đổi partition count** (chạy sau mỗi level):

```bash
change_partitions() {
  local N=$1
  echo "=== Đổi sang $N partitions ==="

  # 1. Xóa connector
  curl -sf -X DELETE http://localhost:8083/connectors/mysql-inventory-connector

  # 2. Xóa Kafka topics
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic inventory.inventory.customers 2>/dev/null || true
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic inventory.inventory.orders 2>/dev/null || true

  # 3. Kill Spark job
  APP_ID=$(curl -s http://localhost:8080/json/ | python -c "
  import sys,json
  apps=json.load(sys.stdin).get('activeapps',[])
  print(apps[0]['id'] if apps else '')
  ")
  [ -n "$APP_ID" ] && curl -sf -X POST http://localhost:8080/app/kill/ -d "id=$APP_ID&terminate=true" || true
  docker exec cdc-spark-master kill $(docker exec cdc-spark-master pgrep -f spark-submit 2>/dev/null) 2>/dev/null || true

  # 4. Recreate topics với partition count mới
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic inventory.inventory.customers \
    --partitions $N --replication-factor 1
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic inventory.inventory.orders \
    --partitions $N --replication-factor 1

  # 5. Xóa checkpoint Spark (quan trọng — checkpoint cũ ghi partition offset cũ)
  MSYS_NO_PATHCONV=1 docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint/cdc-pipeline

  # 6. Đăng ký lại connector
  curl -sf -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @demo/config/connector.json

  # 7. Chờ snapshot xong
  echo "Chờ snapshot Debezium..."
  until [ "$(docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 --list 2>/dev/null | \
    grep -c 'inventory.inventory.customers')" -gt 0 ]; do sleep 3; done
  sleep 10  # Debezium cần thêm thời gian snapshot

  # 8. Submit Spark job mới
  MSYS_NO_PATHCONV=1 docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
    --class CdcRedisConsumer \
    --master spark://cdc-spark-master:7077 \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0" \
    /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar

  # 9. Chờ Spark active
  until curl -s http://localhost:8080/json/ | python -c "
  import sys,json
  sys.exit(0 if json.load(sys.stdin).get('activeapps',[]) else 1)
  " 2>/dev/null; do sleep 5; done

  echo "=== $N partitions sẵn sàng ==="
}
```

**Sau đó chạy full benchmark cho mỗi partition count:**

```bash
for N in 1 2 3 4 6; do
  change_partitions $N
  sleep 30  # Ổn định
  MSYS_NO_PATHCONV=1 docker exec cdc-metrics-exporter python -u \
    /app/benchmark/run_benchmark_v4.py full 2>&1 | tee /tmp/bench_${N}p.log
done
```

**Bảng kết quả cần điền:**

```
| Partitions | Max E2E rec/s | Sustained rec/s | Spark p50 | Bottleneck tại |
|------------|---------------|-----------------|-----------|----------------|
| 1          |               |                 |           |                |
| 2          |               |                 |           |                |
| 3          |               |                 |           |                |
| 4          |               |                 |           |                |
| 6          |               |                 |           |                |
```

---

### Phase 3 — Tìm bottleneck chính xác

Dùng các lệnh sau **trong khi** benchmark đang chạy ở tải cao (inject tại ngưỡng bottleneck):

#### 3a. Debezium throughput

```bash
# Events per second mà Debezium gửi vào Kafka
docker exec cdc-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group connect-mysql-inventory-connector 2>/dev/null | head -10

# Offset tích lũy theo thời gian (chạy 2 lần cách 10s)
docker exec cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic inventory.inventory.customers 2>/dev/null
```

#### 3b. Spark batch time

```bash
# Spark REST API — batch duration
curl -s http://localhost:4040/api/v1/applications/ 2>/dev/null | python -c "
import sys,json
apps=json.load(sys.stdin)
if apps: print('App ID:', apps[0]['id'])
"
# Sau đó:
curl -s "http://localhost:4040/api/v1/applications/<APP_ID>/streaming/statistics" 2>/dev/null
```

#### 3c. MongoDB write throughput

```bash
# mongostat — writes per second
docker exec cdc-mongodb mongostat --host localhost --username root --password root \
  --authenticationDatabase admin --rowcount 10 1 2>/dev/null | \
  grep -v "^host" | awk '{print "inserts/s:", $1, "updates/s:", $2}'
```

#### 3d. Redis throughput

```bash
# redis-cli INFO stats
docker exec cdc-redis redis-cli INFO stats 2>/dev/null | grep -E "instantaneous_ops_per_sec|total_commands_processed"
```

#### 3e. Kafka consumer lag (real-time)

```bash
# Lag của Spark Streaming consumer
docker exec cdc-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group spark-kafka-source-* 2>/dev/null | head -20
# hoặc nếu consumer group name khác:
docker exec cdc-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list 2>/dev/null | grep -v connect
```

#### 3f. Prometheus metrics (nhanh nhất)

```bash
# Lấy toàn bộ metrics một lần
curl -s http://localhost:8000/metrics 2>/dev/null | \
  grep -E "cdc_kafka|cdc_lag|cdc_spark|cdc_mongo" | grep -v "^#"
```

**Checklist xác định bottleneck:**

```
[ ] Debezium lag tích lũy (→ bottleneck ở MySQL binlog hoặc Debezium thread)
[ ] Kafka lag tích lũy nhưng Debezium OK (→ bottleneck ở Spark consumer)
[ ] Spark batch > 5s (→ Spark xử lý chậm hơn trigger interval)
[ ] MongoDB write queue > 1000ms (→ MongoDB là bottleneck)
[ ] Redis ops/s plateau (→ Redis là bottleneck — ít xảy ra)
```

---

### Phase 4 — Sustained test 10 phút

Sau khi tìm được bottleneck, chạy sustained test tại **80% ngưỡng bottleneck** × 10 phút:

```bash
# Ví dụ nếu bottleneck tại 3000 rec/s → chạy 2400 rec/s × 10 phút
# Sửa tạm trong benchmark script hoặc dùng custom inject:

python - <<'EOF'
import subprocess, time, mysql.connector

conn = mysql.connector.connect(
    host='localhost', port=3306,
    user='root', password='root', database='inventory'
)
cur = conn.cursor()
rate = 2400   # rec/s — đổi theo kết quả Phase 1
duration = 600  # 10 phút
interval = 1.0 / rate
base_id = 2_000_000

print(f"Inject {rate} rec/s × {duration}s")
start = time.time()
i = 0
while time.time() - start < duration:
    cur.execute(
        "INSERT INTO customers (id, name, email, phone) VALUES (%s, %s, %s, %s)",
        (base_id + i, f"S{i}", f"s{i}@test.com", "0900000000")
    )
    conn.commit()
    i += 1
    elapsed = time.time() - start
    expected = i * interval
    drift = expected - elapsed
    if drift > 0:
        time.sleep(drift)
    if i % 1000 == 0:
        print(f"t={elapsed:.0f}s injected={i} rate={i/elapsed:.1f}/s")

print(f"DONE: {i} records in {time.time()-start:.1f}s")
cur.close()
conn.close()
EOF
```

**Trong khi inject, monitor mỗi 30s:**

```bash
# Chạy trong terminal khác song song
while true; do
  TS=$(date +%H:%M:%S)
  MONGO=$(docker exec cdc-mongodb mongosh --quiet --eval \
    "db.getSiblingDB('inventory').customers.countDocuments({_id: {\$gte: 2000000}})" \
    inventory 2>/dev/null)
  REDIS_OPS=$(docker exec cdc-redis redis-cli INFO stats 2>/dev/null | \
    grep instantaneous_ops_per_sec | cut -d: -f2 | tr -d '\r')
  LAG=$(curl -s http://localhost:8000/metrics 2>/dev/null | \
    grep 'cdc_lag_total{' | awk '{print $2}')
  echo "$TS | mongo_synced=$MONGO | redis_ops=$REDIS_OPS | lag=$LAG"
  sleep 30
done
```

**Số liệu cần ghi:**

```
Sustained test (___rec/s × 10 phút):
  t=0min:    mongo_synced=___, lag=___
  t=2min:    mongo_synced=___, lag=___
  t=5min:    mongo_synced=___, lag=___
  t=10min:   mongo_synced=___, lag=___
  
  Throughput ổn định? Y/N
  Lag tích lũy? Y/N — nếu Y: tốc độ tích lũy ~___ rec/s
  Kết luận: pipeline có thể duy trì ___ rec/s liên tục
```

---

## Cách ghi kết quả

### Sau mỗi phase, thêm vào `docs/BENCHMARK_RESULTS.md`:

**Thêm section mới** (đầu file sau header, hoặc sau section 4.5):

```markdown
## Run [YYYY-MM-DD] — Bottleneck Hunting — [CPU], [N] partitions

### Phase 1: Stress Test (3 partitions)
| Inject | E2E rec/s | Kafka lag cuối | Spark p50 | Bottleneck |
|--------|-----------|----------------|-----------|------------|
| 100    |           |                |           |            |
| 500    |           |                |           |            |
| 1000   |           |                |           |            |
| 2000   |           |                |           |            |
| 3000   |           |                |           |            |
| 5000   |           |                |           |            |

**Bottleneck bắt đầu:** ___ rec/s  
**Stage bottleneck:** Debezium / Kafka / Spark / MongoDB / Redis  
**Dấu hiệu:** (mô tả)

### Phase 2: Partition Sweep (full mode)
| Partitions | Max E2E | Sustained | Spark p50 | Bottleneck at |
|------------|---------|-----------|-----------|---------------|
| 1          |         |           |           |               |
| 2          |         |           |           |               |
| 3          |         |           |           |               |
| 4          |         |           |           |               |
| 6          |         |           |           |               |

**Quan sát:** Mỗi partition thêm → tăng ___% throughput  
**Diminishing return bắt đầu từ:** ___ partitions

### Phase 3: Bottleneck xác nhận
- Debezium throughput max: ___ events/s
- Spark max batch: ___ms (tại ___ rec/s)
- MongoDB write rate max: ___ ops/s
- Redis ops max: ___ ops/s
- **Bottleneck chính:** ___

### Phase 4: Sustained 10 phút (___ rec/s)
- Throughput ổn định: Y/N
- Lag sau 10 phút: ___
- Kết luận: pipeline giữ ___ rec/s liên tục không lag
```

---

## Commit sau mỗi phase

```bash
# Sau Phase 1:
git add docs/BENCHMARK_RESULTS.md benchmark/results/
git commit -m "test: stress test 3p — bottleneck tại X rec/s (stage: Y)"

# Sau Phase 2:
git add docs/BENCHMARK_RESULTS.md benchmark/results/
git commit -m "test: partition sweep 1→6p — max throughput per partition"

# Sau hoàn thành:
git add docs/BENCHMARK_RESULTS.md benchmark/results/
git commit -m "test: bottleneck hunting complete — sustained cap = X rec/s, stage = Y"
```

---

## Lưu ý kỹ thuật quan trọng

1. **MSYS_NO_PATHCONV=1** — prefix mọi `docker exec` có Unix path `/opt/...`
2. **Dùng Git Bash** — không dùng PowerShell cho curl/bash
3. **Sau mỗi `change_partitions()`** — phải xóa Spark checkpoint `/tmp/spark-checkpoint/cdc-pipeline`, không thì Spark replay offset cũ
4. **Debezium `snapshot.mode=initial`** — mỗi lần restart connector sẽ snapshot toàn bộ MySQL → Kafka, mất vài giây
5. **MongoDB cleanup** — sau sustained test dài, xóa test data: `docker exec cdc-mongodb mongosh inventory --eval "db.customers.deleteMany({_id: {\$gte: 1000000}})"`
6. **Kafka `auto.create.topics.enable=false`** — phải create topics thủ công với đúng partition count trước khi đăng ký connector
7. **Spark packages download** — lần đầu submit sau container restart mất 3–5 phút (ivy2 cache không persist). Chờ `Active: 1` trên Spark Master UI.

---

## Kết quả mong đợi (hypothesis trước khi test)

Dựa trên data đã có:
- **3 partitions**: bottleneck ước tính ~2500–4000 rec/s (dựa trên VM 12p cho 1633 rec/s, laptop 3p dưới 2000 không thấy bottleneck)
- **1 partition**: bottleneck ~1000 rec/s (đã đo 2026-05-25)
- **6 partitions**: bottleneck ước tính ~3000–5000 rec/s
- **Stage bottleneck chính**: Debezium (bottleneck cũ ở 1p là Debezium/Kafka, không phải Spark)

Nếu tăng partition không tăng throughput → bottleneck là Debezium (single thread per connector)
Nếu tăng partition tăng throughput tuyến tính → bottleneck là Kafka consumer (Spark)

---

## Sau session này, cần ghi vào memory

1. Bottleneck stage chính xác
2. Throughput cap thật của laptop (rec/s sustained không lag)
3. Partition count optimal (point of diminishing return)
4. Sustained throughput (10 phút, không suy giảm)
