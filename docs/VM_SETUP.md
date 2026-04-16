# Deploy CDC Pipeline lên VM thuê

Hướng dẫn chạy pipeline trên VM cloud (AWS EC2, GCP, Azure, DigitalOcean, Vultr...) để benchmark scale thực tế.

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
# Docker
curl -fsSL https://get.docker.com | sudo sh

# Thêm user vào group docker (để không cần sudo)
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version
```

### 3. Cài Python + dependencies

```bash
sudo apt install -y python3 python3-pip
pip3 install --break-system-packages pymysql pymongo redis kafka-python prometheus-client
```

### 4. Cài sbt (nếu cần rebuild JAR Scala)

```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | sudo gpg --dearmor -o /usr/share/keyrings/sbt.gpg
sudo apt update
sudo apt install -y sbt default-jdk
sbt --version
```

### 5. Clone project

```bash
cd ~
git clone <your-repo-url> cdc-pipeline
cd cdc-pipeline
```

### 6. Khởi động

```bash
bash start.sh
```

---

## Scale lên 6 hoặc 8 workers

Mặc định `docker-compose.yml` có 3 workers (`spark-worker-1, 2, 3`). Để tăng:

### Cách 1: Thêm worker vào docker-compose.yml

Edit `pipeline/docker-compose.yml`, copy block `spark-worker-3` rồi đổi tên + port:

```yaml
spark-worker-4:
  image: cdc-spark:3.5.0
  build:
    context: ../spark
    dockerfile: Dockerfile
  container_name: cdc-spark-worker-4
  networks:
    - cdc-net
  depends_on:
    spark-master:
      condition: service_healthy
  command: >
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://cdc-spark-master:7077
  ports:
    - "8086:8081"    # Port UI khác nhau
  volumes:
    - ../jobs:/opt/spark/jobs

# Tương tự cho spark-worker-5, 6, 7, 8...
```

Rồi khởi động lại:

```bash
bash stop.sh
bash start.sh
```

### Cách 2: Limit resource của worker

Trong `docker-compose.yml`, thêm environment cho worker:

```yaml
environment:
  SPARK_WORKER_CORES: 4      # Mỗi worker dùng 4 cores
  SPARK_WORKER_MEMORY: 4G    # Mỗi worker dùng 4GB RAM
```

VM 16 cores → 8 workers × 2 cores hoặc 4 workers × 4 cores.

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

## Benchmark scale

### 1. Chạy baseline (1 partition, 3 workers)

```bash
bash benchmark/benchmark_scaling.sh --quick
```

Kết quả sẽ ghi vào `benchmark/results/scaling_<timestamp>.json`.

### 2. Tăng partition + Rebuild JAR nếu cần

```bash
# Đổi Kafka topic sang 3 partitions
docker exec cdc-kafka kafka-topics --alter \
  --bootstrap-server localhost:9092 \
  --topic inventory.inventory.customers --partitions 3
```

### 3. Chạy benchmark lại, so sánh

```bash
bash benchmark/benchmark_scaling.sh
```

### 4. Tải kết quả về máy local

```bash
# Trên máy local
scp user@vm-ip:~/cdc-pipeline/benchmark/results/*.json ./benchmark/results/
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
