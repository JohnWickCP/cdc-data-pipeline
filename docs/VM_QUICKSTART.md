# VM Quickstart — CDC Data Pipeline

Tài liệu 1 trang, dùng khi SSH vào VM để thao tác nhanh.  
Tài liệu đầy đủ: `docs/VM_SETUP.md`

---

## 0. Thông số VM (điền trước khi demo)

```
Provider  : _______________   (GCP / AWS / DO / Vultr / ...)
Instance  : _______________   (vd: c2-standard-16)
vCPU      : ___    RAM: ___ GB    Disk: ___ GB
Region    : _______________   (vd: asia-southeast1 / sgp1)
IP Public : _______________
OS        : Ubuntu 22.04 LTS
Thuê từ   : ___/___/2026      Trả lúc: ___:___
Chi phí   : ~$___ / giờ  →  ước tính $___  cho ___ giờ

Ghi chú   : ______________________________________________
```

---

## 1. Cài đặt lần đầu (~5 phút)

```bash
# Clone
git clone <your-repo-url> cdc-data-pipeline
cd cdc-data-pipeline

# Setup tất cả tự động (Docker + Python deps + env)
bash scripts/vm/vm_setup.sh
```

> `vm_setup.sh` tự cài Docker nếu chưa có, cài Python packages,
> tạo `demo/.env`, và in hướng dẫn bước tiếp theo.

---

## 2. Khởi động pipeline

```bash
# Xem profile sẽ dùng (tham khảo)
bash start.sh --detect

# Khởi động với profile VM (16vCPU/32GB)
bash start.sh --profile=vm

# Chờ 5-8 phút lần đầu (Spark download packages)
# Smoke test
bash scripts/test_smoke.sh  # mong đợi: 43/43 PASS
```

---

## 3. Chạy demo

**Terminal 1 — Demo server:**
```bash
cd demo && bash run_demo.sh
# Truy cập: http://<IP_PUBLIC>:8888
```

**Terminal 2 — Recorder (ghi metrics):**
```bash
python3 demo/record_demo.py
# Ctrl+C khi xong → tự in summary + lưu demo/recordings/demo_*.jsonl
```

**Terminal 3 — Benchmark (tùy chọn):**
```bash
bash scripts/run_bench.sh           # quick ~3 phút
bash scripts/run_bench.sh full      # full ~10 phút
```

---

## 4. Scale Kafka (tùy chọn — để thử giới hạn)

```bash
bash demo/kafka_scale.sh status          # xem broker hiện tại
bash demo/kafka_scale.sh add             # thêm kafka-2 + kafka-3
bash demo/kafka_scale.sh rebalance       # tăng partitions + restart Spark
bash demo/kafka_scale.sh remove          # thu về 1 broker
```

---

## 5. Ports cần mở trên firewall VM

| Port | Service | Bắt buộc? |
|------|---------|-----------|
| **8888** | Demo dashboard | Bắt buộc |
| **3000** | Grafana | Nên mở |
| **8080** | Spark Master UI | Tùy chọn |
| 9090 | Prometheus | Nội bộ thôi |
| ~~3306~~ | MySQL | **KHÔNG mở** |
| ~~27017~~ | MongoDB | **KHÔNG mở** |
| ~~6379~~ | Redis | **KHÔNG mở** |
| ~~9092~~ | Kafka | **KHÔNG mở** |

```bash
# GCP: thêm firewall rule tag (thay YOUR_VM_TAG)
gcloud compute firewall-rules create cdc-demo \
  --allow tcp:8888,tcp:3000,tcp:8080 \
  --target-tags YOUR_VM_TAG

# AWS: Security Group → Inbound → Custom TCP: 8888, 3000, 8080

# DigitalOcean / Vultr: Networking → Firewall → Add rule TCP 8888, 3000, 8080
```

---

## 6. Access URLs (điền IP thật)

```
Demo Dashboard  : http://<IP>:8888
Grafana         : http://<IP>:3000   (admin / admin)
Spark Master UI : http://<IP>:8080
```

---

## 7. Kết quả benchmark (điền sau khi chạy)

```
Ngày chạy  : ___/___/2026    VM: _______________

Profile    : vm  (___vCPU / ___GB RAM / ___ Kafka brokers)

| Test case          | E2E (rec/s) | Kafka lag (peak) | Spark batch (avg) |
|--------------------|-------------|------------------|-------------------|
| quick-100          |             |                  |                   |
| quick-200          |             |                  |                   |
| quick-500          |             |                  |                   |
| full-sustained     |             |                  |                   |
| 3-broker (scale)   |             |                  |                   |

Ghi chú:
_______________________________________________________________
_______________________________________________________________
```

Kết quả chi tiết: `benchmark/results/history.jsonl`  
So sánh với laptop: `python benchmark/compare_runs.py -n 10`

---

## 8. Tải kết quả về máy local

```bash
# Chạy từ máy local
scp user@<IP>:~/cdc-data-pipeline/benchmark/results/history.jsonl \
    ./benchmark/results/vm_history.jsonl

scp -r user@<IP>:~/cdc-data-pipeline/demo/recordings/ \
    ./demo/recordings/vm/

# So sánh
python benchmark/compare_runs.py -n 15
```

---

## 9. Tắt VM (đừng quên!)

```bash
# Trước khi tắt — export kết quả (xem bước 8)

# Tắt pipeline giữ data
bash stop.sh

# Hoặc tắt sạch
bash stop.sh -v
```

**Xóa VM trên provider sau khi xong để tránh charge thêm!**
