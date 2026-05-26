# Stress Test Guide — CDC Capacity Test

Hướng dẫn chạy Capacity Test để tìm điểm bão hòa của pipeline.

---

## Nhanh: cái gì cần restart, cái gì không

| Thay đổi | Cách áp dụng | Thời gian |
|---|---|---|
| Total executor cores | UI → **Resubmit Spark** | ~30s |
| Executor memory | UI → **Resubmit Spark** | ~30s |
| `STRESS_RATES` / `STRESS_SAT_RATIO` | Sửa `demo/.env` → restart `demo_server.py` | ~5s |
| `KAFKA_NUM_PARTITIONS` | `bash stop.sh -v && bash start.sh` | ~5 phút |
| `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` | `bash stop.sh -v && bash start.sh` | ~5 phút |

---

## Config mặc định theo môi trường

### Laptop (`demo/.env` copy từ `.env.laptop`)
```ini
STRESS_RATES=50,100,150,200,250,300,400
STRESS_DURATION=40
STRESS_SAT_RATIO=0.5
```
Stack (`bash start.sh`):
```ini
KAFKA_NUM_PARTITIONS=3
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=2g
```

### VM (`demo/.env` copy từ `.env.vm` hoặc set thủ công)
```ini
STRESS_RATES=500,1000,2000,5000,10000
STRESS_DURATION=60
STRESS_SAT_RATIO=0.5
```
Stack:
```ini
KAFKA_NUM_PARTITIONS=3
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=4g
```

---

## Workflow chuẩn — một buổi test

### Bước 1: Khởi động stack
```bash
# Lần đầu hoặc đổi KAFKA_NUM_PARTITIONS/SPARK_WORKER_CORES
bash stop.sh -v && bash start.sh
```

### Bước 2: Khởi động demo server
```bash
cd demo
# Tạo .env nếu chưa có (copy từ example, chỉnh STRESS_RATES nếu cần)
cp .env.example .env
python demo_server.py
```
Mở: http://localhost:8888 → tab **⚡ Capacity Test**

### Bước 3: Xác nhận "Config đang chạy"
Card đầu tab sẽ hiển thị:
- **Kafka Partitions** — phải là 3 (màu xanh), nếu 1 (màu cam) → cần `stop.sh -v && start.sh`
- **Spark Workers / Total Cores** — phải khớp với `.env.laptop`

### Bước 4: Chạy test
Bấm **RUN CAPACITY TEST**. Kết quả xuất hiện từng level, bảng cuối trang hiển thị khi done.

---

## Các kịch bản test so sánh

### So sánh Spark cores (không cần restart stack)

1. Chạy test với **Auto cores** (mặc định, dùng hết)
2. UI → *Spark Tuning* → chọn **4 cores** → **Resubmit Spark Job** → đợi ~30s
3. Chạy test lại → so sánh bảng kết quả

Câu hỏi trả lời được: *"Thêm cores cải thiện throughput bao nhiêu?"*

### So sánh Kafka partitions (cần restart stack)

```bash
# Test với 1 partition
# Sửa .env.laptop: KAFKA_NUM_PARTITIONS=1
bash stop.sh -v && bash start.sh
# → chạy test, ghi lại kết quả

# Test với 3 partitions
# Sửa .env.laptop: KAFKA_NUM_PARTITIONS=3
bash stop.sh -v && bash start.sh
# → chạy test lại, so sánh
```

Câu hỏi trả lời được: *"3 Kafka partitions tăng throughput bao nhiêu so với 1?"*

### Test trên VM sau khi deploy

1. Copy `demo/.env.example` → `demo/.env`
2. Đặt IP của VM:
   ```ini
   MYSQL_HOST=<VM_IP>
   REDIS_HOST=<VM_IP>
   MONGO_URI=mongodb://<VM_IP>:27017
   PROMETHEUS_URL=http://<VM_IP>:9090
   ```
3. Đặt rates cho VM:
   ```ini
   STRESS_RATES=500,1000,2000,5000,10000
   STRESS_DURATION=60
   ```
4. Chạy `python demo_server.py` trên máy local, pipeline chạy trên VM

---

## Đọc kết quả

| Cột | Ý nghĩa |
|---|---|
| Rate | Tốc độ inject mục tiêu (rec/s) |
| Inject Rate | Thực tế MySQL nhận được |
| Kafka Rate | Tốc độ Debezium → Kafka |
| Mongo Rate | Tốc độ Spark → MongoDB |
| Consumer Lag (>N) | Lag trung bình / threshold bão hòa |
| Batch ms | Spark batch duration trung bình |
| Status | ✅ Stable / ⚠ Saturating |

**Điểm bão hòa (saturation)** khi một trong hai:
- `avg_consumer_lag > max(50, rate × SAT_RATIO)` — lag vượt ngưỡng tỉ lệ
- Lag tăng liên tục 3 mẫu liên tiếp + lag > 20

**Bottleneck thường gặp:**
- `Inject Rate << Rate` → MySQL bị nghẽn (quá nhiều connection/commit)
- `Kafka Rate << Inject Rate` → Debezium lag sau MySQL
- `Mongo Rate << Kafka Rate` → Spark xử lý chậm (cores, memory, batch size)
- `Consumer Lag tăng đều` → Spark không drain kịp

---

## Ghi chú kỹ thuật

- **`maxOffsetsPerTrigger`** không thể thay đổi mà không recompile JAR (là Kafka source option trong Scala, không phải Spark conf). Workaround: dùng `--total-executor-cores` để throttle Spark.
- Trigger interval cố định 5s trong cả Scala JAR và Python fallback.
- Sau khi **Resubmit Spark**, job cần ~30s để khởi động lại (download packages nếu cache miss = 3-5 phút). Xem Spark UI: http://localhost:8080.
- Benchmark so sánh nhiều runs → dùng `python benchmark/compare_runs.py` (lưu vào `benchmark/results/history.jsonl`).
