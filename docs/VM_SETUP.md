# Deploy CDC Pipeline lên Cloud VM

Hướng dẫn từng bước để chạy pipeline trên VM cloud Linux (Ubuntu 22.04).
Áp dụng cho: AWS EC2, GCP Compute Engine, DigitalOcean Droplet, Vultr, Azure VM.

---

## Mục tiêu

Chứng minh khả năng scale của hệ thống bằng cách:
1. Chạy pipeline trên VM có **nhiều cores** hơn máy local
2. Tăng số Spark workers (từ 3 lên 6-8)
3. Tăng Kafka partitions (từ 1 lên 3-6)
4. So sánh throughput giữa local và cloud → có số liệu cho báo cáo đồ án

---

## Cấu hình VM khuyến nghị

| Provider | Instance | vCPU | RAM | Giá/giờ |
|----------|----------|------|-----|---------|
| AWS | `c6i.4xlarge` | 16 | 32GB | ~$0.68 |
| GCP | `c2-standard-16` | 16 | 64GB | ~$0.81 |
| DigitalOcean | `c-16` CPU-Optimized | 16 | 32GB | ~$0.48 |
| Vultr | `vhp-16c-32gb` | 16 | 32GB | ~$0.38 |

**Tối thiểu:** 8 vCPU, 16GB RAM. Dưới mức này Spark cluster không có đủ resource.

**Khuyến nghị benchmark:** 16 vCPU, 32GB RAM. Dùng 4-8 giờ là đủ test.

---

## Cài đặt từ đầu

### 1. Tạo VM + SSH vào

Chọn **Ubuntu 22.04 LTS** khi tạo VM. Sau khi SSH vào:

```bash
sudo apt update && sudo apt upgrade -y
```

### 2. Cài Docker + Docker Compose

```bash
# Cài Docker Engine (script chính thức)
curl -fsSL https://get.docker.com | sudo sh

# Thêm user vào group docker (để không cần sudo mỗi lần)
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version        # Phải ≥ 24.0
docker compose version  # Phải ≥ 2.0
```

### 3. Cài Git + Python

```bash
sudo apt install -y git python3 python3-pip

# Cài Python dependencies cho demo server và benchmark
pip3 install --break-system-packages \
  pymysql pymongo redis kafka-python \
  prometheus-client flask flask-cors requests
```

### 4. Clone project

```bash
cd ~
git clone <your-repo-url> cdc-data-pipeline
cd cdc-data-pipeline
```

### 5. Chỉnh cấu hình cho VM (nếu RAM ≥ 32GB)

File `.env.server` đã có sẵn cho workstation 32GB+. `start.sh` tự detect hardware:

```bash
# Xem start.sh sẽ dùng profile nào
bash start.sh --detect

# Force dùng profile server nếu VM có ≥ 32GB RAM
bash start.sh --profile=server
```

Nếu VM có cấu hình khác (ví dụ 16 vCPU, 64GB), có thể sửa `.env.server`:
```bash
# Tăng số core và memory cho Spark workers
SPARK_WORKER_CORES=8
SPARK_WORKER_MEMORY=8g
```

### 6. Khởi động pipeline

```bash
bash start.sh
# Chờ 3–5 phút. Script tự: pull images → start containers → register connector → submit Spark job
```

Verify mọi thứ OK:
```bash
bash test_smoke.sh
# Kết quả mong đợi: 43/43 PASS
```

---

## Scale lên nhiều Spark workers hơn

Mặc định `docker-compose.yml` có 3 workers (`spark-worker-1, 2, 3`). VM mạnh hơn có thể chạy nhiều hơn.

### Thêm worker mới vào docker-compose.yml

Copy block `spark-worker-3` trong `docker-compose.yml`, đổi tên + port:

```yaml
  spark-worker-4:
    image: bitnami/spark:3.5.0
    container_name: cdc-spark-worker-4
    networks:
      - cdc-net
    depends_on:
      spark-master:
        condition: service_healthy
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://cdc-spark-master:7077
      - SPARK_WORKER_CORES=4
      - SPARK_WORKER_MEMORY=4G
    ports:
      - "8086:8081"
    volumes:
      - ./jobs:/opt/spark/jobs
```

Sau đó restart:
```bash
bash stop.sh
bash start.sh
```

### Điều chỉnh resource mỗi worker (trong .env.server)

```bash
SPARK_WORKER_CORES=6       # VM 16 vCPU → 4 workers × 4 cores
SPARK_WORKER_MEMORY=8g     # VM 32GB RAM → 4 workers × 8GB
```

---

## Scale Kafka partitions

Để Spark cluster xử lý song song, Kafka topic cần nhiều partitions tương ứng.

### Cách 1: Đổi config mặc định trong `demo/connector.json`

Thêm:
```json
"topic.creation.default.partitions": "6",
"topic.creation.default.replication.factor": "1"
```

### Cách 2: Thay đổi partition cho topic hiện có

```bash
docker exec cdc-kafka kafka-topics \
  --alter \
  --bootstrap-server localhost:9092 \
  --topic inventory.inventory.customers \
  --partitions 6
```

*(Không giảm được partition, chỉ tăng)*

---

## Chạy benchmark

### 1. Chạy quick benchmark

```bash
bash run_bench.sh           # Quick mode (~3 phút)
bash run_bench.sh full      # Full mode (~10 phút)
```

Kết quả append vào `benchmark/results/history.jsonl`.

### 2. Tăng Kafka partition (optional)

```bash
# Tăng từ 1 lên 3 partitions (chỉ tăng được, không giảm)
docker exec cdc-kafka kafka-topics --alter \
  --bootstrap-server localhost:9092 \
  --topic inventory.inventory.customers --partitions 3

docker exec cdc-kafka kafka-topics --alter \
  --bootstrap-server localhost:9092 \
  --topic inventory.inventory.orders --partitions 3
```

Sau đó restart Spark job để nó detect partition mới:
```bash
# Xem PID của Spark driver
docker exec cdc-spark-master bash -c "ps aux | grep CdcRedisConsumer"

# Kill và submit lại (start.sh có sẵn step này)
bash start.sh   # Idempotent — tự detect và resubmit nếu cần
```

### 3. Tải kết quả về máy local

```bash
# Từ máy local (thay user và vm-ip)
scp user@vm-ip:~/cdc-data-pipeline/benchmark/results/history.jsonl ./benchmark/results/vm_history.jsonl

# So sánh
python benchmark/compare_runs.py -n 10
```

---

## Mở port cho remote access

Để truy cập Grafana/Spark UI từ máy local qua IP của VM:

### Option 1: SSH Tunnel (an toàn, khuyến nghị)

Từ máy local:
```bash
ssh -L 3000:localhost:3000 -L 8080:localhost:8080 user@vm-ip
```

Mở browser ở máy local: `http://localhost:3000`

### Option 2: Mở firewall (KHÔNG an toàn nếu port không có auth)

```bash
# Chỉ làm trong môi trường test, tắt khi xong
sudo ufw allow 3000
sudo ufw allow 8080
```

Rồi truy cập trực tiếp `http://<vm-ip>:3000`.

---

## Ước tính chi phí benchmark

| Mục | Thời gian | Chi phí (DO c-16) |
|-----|-----------|---------------------|
| Setup VM + Docker | 30 phút | $0.25 |
| Build JAR + khởi động pipeline | 15 phút | $0.12 |
| Chạy 4-5 benchmark configs | 2 giờ | $0.96 |
| Chụp screenshot Grafana + tải kết quả | 30 phút | $0.25 |
| **Tổng** | **~3-4 giờ** | **~$1.6-2** |

Đừng quên **xóa VM** sau khi xong để không bị charge thêm!

```bash
# Export kết quả trước khi xóa VM
scp user@vm-ip:~/cdc-pipeline/benchmark/results/ ./local-results/
```

---

## Lưu ý bảo mật

1. **Không commit `.env`** có credentials thật
2. **Thay password mặc định** của MySQL/Mongo/Redis/Grafana trước khi deploy
3. **Không expose port 3306, 27017, 6379, 9092** ra Internet (chỉ port 3000, 8080 cần)
4. Cân nhắc dùng **VPN hoặc SSH tunnel** thay vì mở firewall

---

## Troubleshooting trên VM

### Container bị kill do OOM (Out of Memory)

```bash
docker stats
```

Giảm Spark worker memory:
```yaml
environment:
  SPARK_WORKER_MEMORY: 2G
```

### Disk full

```bash
df -h
docker system prune -a --volumes   # CẨN THẬN: xóa sạch
```

### Network latency cao (VM ở xa)

Đo test: `ping vm-ip`. Nếu > 100ms, benchmark có thể sai. Chọn VM ở region gần (Singapore cho Việt Nam).
