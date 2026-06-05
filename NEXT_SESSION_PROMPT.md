<!--
  Prompt cho session MIXED WORKLOAD + MULTI-TABLE — xóa sau khi dùng.
  Ước tính thời gian: 60–90 phút chạy thật + phân tích.
-->

# Session: Mixed Workload + Multi-table Test

Đọc memory `bottleneck_hunting_results.md` + `session_handoff_fixes.md` + `project_overview.md` trước.

---

## Quyền và phạm vi

**Bạn có toàn quyền bypass** — chạy thoải mái:
- `docker compose up/down/restart` bất kỳ service nào
- Chạy benchmark bất kỳ bao lâu
- Inject data trực tiếp vào MySQL
- Không cần xin phép trước mỗi bước

**Session này được phép chạy lâu** — đừng tóm tắt sớm, đừng lo về context limit.

---

## Bối cảnh — Những gì đã biết

### Kết quả từ session trước (2026-06-05, bottleneck hunting)

| Metric | Giá trị |
|--------|---------|
| Max E2E (3p, INSERT-only) | **2,270 rec/s** (bottleneck_hunting mode) |
| Sustained 10 phút (INSERT-only) | **1,248 rec/s** (lag=0, 100% sync) |
| MySQL inject cap | **~3,400 rec/s** (WSL2/Docker hardware limit) |
| Bottleneck stage | MySQL → Debezium (single-thread) |
| E2E latency P50 | **4,883ms** (~1 Spark trigger cycle) |
| Spark p50/p95 | 858/1145ms (healthy tại 1,289 rec/s) |
| Partition ảnh hưởng | Không đáng kể (4-7% với 1p→6p) |

### Những gì **chưa** đo
1. **Mixed workload** (INSERT + UPDATE + DELETE đồng thời)
2. **Multi-table** (customers + orders cùng lúc)

---

## Mục tiêu session này

1. **Mixed workload** — `realistic` mode đã có code, chỉ cần chạy:
   - 60% INSERT / 30% UPDATE / 10% DELETE
   - So sánh throughput với INSERT-only baseline
   - Câu hỏi: UPDATE/DELETE có làm chậm pipeline không? Bao nhiêu %?

2. **Multi-table** — inject đồng thời customers + orders:
   - Viết script inject song song 2 bảng
   - Đo tổng throughput, Kafka lag, MongoDB write rate
   - Câu hỏi: 2 tables → pipeline có bị nghẽn ở điểm nào khác không?

3. **Ghi kết quả** vào `docs/BENCHMARK_RESULTS.md`

---

## Kế hoạch thực hiện

### Phase 0 — Khởi động pipeline sạch

```bash
bash scripts/test_smoke.sh
# Phải 43/43 PASS trước khi bắt đầu
```

Nếu pipeline chưa chạy: `bash start.sh`
Nếu Spark job chết: submit lại thủ công (xem CLAUDE.md)

Kiểm tra 3 partitions (canonical setup):
```bash
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic inventory.inventory.customers 2>/dev/null | grep PartitionCount
```

---

### Phase 1 — Mixed Workload (`realistic` mode)

Mode này đã có sẵn trong `benchmark/run_benchmark_v4.py`. Levels: [100, 200, 500], duration 30s.

```bash
MSYS_NO_PATHCONV=1 docker exec cdc-metrics-exporter python -u \
  /app/benchmark/run_benchmark_v4.py realistic 2>&1
```

**Output của mode này khác full mode** — có breakdown INSERT/UPDATE/DELETE:
```
Inject rate:    X events/s
  INSERT:        60%
  UPDATE:        30%
  DELETE:        10%
E2E records/s: X  ← Kafka delta / total_time
```

**So sánh với baseline INSERT-only (từ session trước):**

| Level | INSERT-only E2E | Mixed E2E | % change |
|-------|----------------|-----------|----------|
| 100   | 93.6           |           |          |
| 200   | 175.9          |           |          |
| 500   | 425.8          |           |          |
| Sustained | 1,248.4   |           |          |

**Câu hỏi cần trả lời:**
- Mixed có làm giảm throughput không, và giảm bao nhiêu %?
- UPDATE events có throughput tương đương INSERT không? (chúng đi qua cùng Spark job)
- DELETE events có làm Redis key bị xóa đúng không? (đã verify: YES)

---

### Phase 2 — Multi-table concurrent injection

Viết script inject đồng thời cả `customers` và `orders` bằng 2 thread:

```python
# multi_table_inject.py — chạy từ host, inject 2 bảng song song
import threading, pymysql, time, random

MYSQL_CONFIG = dict(host='127.0.0.1', port=3306,
                    user='root', password='root',
                    database='inventory', autocommit=True)
BASE_CUST = 2_000_000
BASE_ORD  = 5_000_000
RATE      = 500   # rec/s PER TABLE → tổng 1000 events/s vào pipeline
DURATION  = 60    # giây

def inject_customers(rate, duration):
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    interval = 1.0 / rate
    i = 0
    start = time.time()
    while time.time() - start < duration:
        cur.execute(
            "INSERT INTO customers (id,name,email,phone) VALUES (%s,%s,%s,%s)",
            (BASE_CUST+i, f"MT_C{i}", f"c{i}@mt.com", "0911111111")
        )
        i += 1
        drift = (i * interval) - (time.time() - start)
        if drift > 0.001: time.sleep(drift)
    print(f"[customers] {i} records in {time.time()-start:.1f}s = {i/(time.time()-start):.1f}/s")
    conn.close()

def inject_orders(rate, duration):
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    interval = 1.0 / rate
    i = 0
    start = time.time()
    while time.time() - start < duration:
        cust_id = random.randint(1, 3)  # reference existing customers
        cur.execute(
            "INSERT INTO orders (id,order_date,purchaser,quantity,product_id) VALUES (%s,NOW(),%s,%s,%s)",
            (BASE_ORD+i, cust_id, random.randint(1,5), random.randint(101,106))
        )
        i += 1
        drift = (i * interval) - (time.time() - start)
        if drift > 0.001: time.sleep(drift)
    print(f"[orders] {i} records in {time.time()-start:.1f}s = {i/(time.time()-start):.1f}/s")
    conn.close()

print(f"Inject {RATE} cust/s + {RATE} ord/s x {DURATION}s")
t1 = threading.Thread(target=inject_customers, args=(RATE, DURATION))
t2 = threading.Thread(target=inject_orders,   args=(RATE, DURATION))
t1.start(); t2.start()
t1.join();  t2.join()
print("DONE")
```

**Lưu vào `benchmark/multi_table_inject.py` và chạy:**

```bash
python benchmark/multi_table_inject.py
```

**Trong khi inject, monitor mỗi 15s:**
```bash
while true; do
  TS=$(date +%H:%M:%S)
  CUST_MY=$(docker exec cdc-mysql mysql -uroot -proot inventory -Nse \
    "SELECT COUNT(*) FROM customers WHERE id >= 2000000" 2>/dev/null)
  ORD_MY=$(docker exec cdc-mysql mysql -uroot -proot inventory -Nse \
    "SELECT COUNT(*) FROM orders WHERE id >= 5000000" 2>/dev/null)
  CUST_MG=$(docker exec cdc-mongodb mongosh inventory --quiet \
    --eval "db.customers.countDocuments({_id:{\$gte:2000000}})" 2>/dev/null)
  ORD_MG=$(docker exec cdc-mongodb mongosh inventory --quiet \
    --eval "db.orders.countDocuments({_id:{\$gte:5000000}})" 2>/dev/null)
  LAG=$(curl -s http://localhost:8000/metrics 2>/dev/null | grep "^cdc_lag_total " | awk '{print $2}')
  echo "$TS | cust: mysql=$CUST_MY mongo=$CUST_MG | ord: mysql=$ORD_MY mongo=$ORD_MG | lag=$LAG"
  sleep 15
done
```

**Metrics cần đo:**
```
Multi-table test (500 cust/s + 500 ord/s = 1000 total events/s):
  t=0:   customers_lag=___, orders_lag=___
  t=15s: customers_lag=___, orders_lag=___
  t=30s: customers_lag=___, orders_lag=___
  t=60s: customers_lag=___, orders_lag=___
  
  Customers E2E: ___/___ synced, lag=___
  Orders E2E:    ___/___ synced, lag=___
  Tổng throughput: ___ events/s (customers + orders)
  
  So với single-table 1000/s: ___% change
```

---

### Phase 3 — Tăng tải multi-table đến bottleneck

Thử các mức: 500+500, 1000+1000, 1500+1500 rec/s per table. Tìm ngưỡng lag tích lũy.

```python
# Đổi RATE trong multi_table_inject.py, chạy lại mỗi lần
```

**Bảng kết quả:**

| Customers/s | Orders/s | Total events/s | Kafka lag peak | Synced? | Bottleneck |
|-------------|----------|----------------|----------------|---------|------------|
| 500 | 500 | 1000 | | | |
| 1000 | 1000 | 2000 | | | |
| 1500 | 1500 | 3000 | | | |

---

### Phase 4 — Cleanup và kết luận

Xóa test data:
```bash
docker exec cdc-mysql mysql -uroot -proot inventory -e \
  "DELETE FROM customers WHERE id >= 2000000; DELETE FROM orders WHERE id >= 5000000;" 2>/dev/null
```

**Ghi vào `docs/BENCHMARK_RESULTS.md`** — section mới:

```markdown
## Run [YYYY-MM-DD] — Mixed Workload + Multi-table

### Mixed Workload (realistic mode, 3 partitions)
| Level | INSERT-only | Mixed (60/30/10) | Δ% |
|-------|-------------|-------------------|----|
| 100   | 93.6        |                   |    |
| 200   | 175.9       |                   |    |
| 500   | 425.8       |                   |    |

**Kết luận:** UPDATE/DELETE làm giảm throughput ___% (hay không đáng kể)

### Multi-table (customers + orders đồng thời)
| Tổng events/s | Lag peak | Synced | Bottleneck |
|---------------|----------|--------|------------|
| 1000          |          |        |            |
| 2000          |          |        |            |

**Kết luận:** Multi-table bottleneck tại ___events/s (hay cùng ngưỡng single-table)
```

---

## Schema orders table (cần biết trước khi inject)

```bash
docker exec cdc-mysql mysql -uroot -proot inventory \
  -e "DESCRIBE orders;" 2>/dev/null
```

Các cột thường có: `id`, `order_date`, `purchaser` (FK → customers.id), `quantity`, `product_id`

**Lưu ý quan trọng:**
- `purchaser` là FK → customers.id, phải reference valid customer
- Hoặc tắt FK check: `SET FOREIGN_KEY_CHECKS=0` trước khi inject orders

---

## Câu hỏi cần trả lời cuối session

1. **Mixed workload**: Pipeline xử lý UPDATE/DELETE giống INSERT về throughput không?
2. **Multi-table**: 2 tables → Kafka lag có tăng gấp đôi không?
3. **Bottleneck có thay đổi không?** Hay vẫn là MySQL inject speed?
4. **Redis consistency**: Sau mixed workload, `customers:total` counter đúng không? Verify bằng: `redis-cli GET customers:total` vs `MySQL COUNT(*)`

---

## Sau session này, ghi vào memory

1. Mixed workload throughput so với INSERT-only (% giảm)
2. Multi-table throughput ceiling
3. Redis consistency sau mixed workload
4. Bất kỳ bug mới phát hiện
