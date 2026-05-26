#!/bin/bash
# ============================================================
# CDC Pipeline — Full Startup Script
# Xử lý mọi tình huống: cold start, restart, sau down -v
# Bao gồm auto-patch Grafana datasource UID
# Sử dụng: cd ~/cdc-pipeline && bash start_full_pipeline.sh
# ============================================================

set -e

# Ngăn Git Bash trên Windows tự động đổi đường dẫn Unix (/opt/...) thành đường dẫn C:/
export MSYS_NO_PATHCONV=1

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$PROJECT_DIR"

# Tìm Python interpreter — dùng --version để test thật, tránh Windows Store stub
# (python3 trên Windows có thể là stub redirect tới Microsoft Store, không chạy được)
if python3 --version >/dev/null 2>&1; then
    PYTHON=python3
elif python --version >/dev/null 2>&1; then
    PYTHON=python
else
    PYTHON=python3
fi

# ── Auto-detect hardware profile ──────────────────────────
# Chạy khi --profile không được truyền vào.
# Trả về: laptop | server | vm
auto_detect_profile() {
    local os battery ram_gb virt
    os=$(uname -s 2>/dev/null || echo "unknown")

    # Battery → laptop?
    case "$os" in
        MINGW*|MSYS*|CYGWIN*)
            local bs
            bs=$(wmic path Win32_Battery get BatteryStatus /value 2>/dev/null \
                | tr -d '\r' | grep "^BatteryStatus=" | cut -d= -f2 | xargs)
            [ -n "$bs" ] && battery="yes" || battery="no"
            ;;
        Linux*)
            ls /sys/class/power_supply/ 2>/dev/null | grep -qi "bat" \
                && battery="yes" || battery="no"
            ;;
        *) battery="no" ;;
    esac

    # RAM (GB)
    case "$os" in
        MINGW*|MSYS*|CYGWIN*)
            local bytes
            bytes=$(wmic computersystem get TotalPhysicalMemory /value 2>/dev/null \
                | tr -d '\r' | grep "^TotalPhysicalMemory=" | cut -d= -f2 | xargs)
            [[ "$bytes" =~ ^[0-9]+$ ]] \
                && ram_gb=$(( bytes / 1024 / 1024 / 1024 )) || ram_gb=0
            ;;
        Linux*)
            local mb
            mb=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "0")
            ram_gb=$(( mb / 1024 ))
            ;;
        *) ram_gb=0 ;;
    esac

    # Virtualization
    case "$os" in
        MINGW*|MSYS*|CYGWIN*)
            local combined
            combined=$(wmic computersystem get model,manufacturer /value 2>/dev/null \
                | tr -d '\r' | tr '[:upper:]' '[:lower:]')
            case "$combined" in
                *virtualbox*|*vmware*|*"hyper-v"*|*"virtual machine"*|*kvm*|*qemu*)
                    virt="vm" ;;
                *) virt="none" ;;
            esac
            ;;
        Linux*)
            local sdv
            sdv=$(systemd-detect-virt 2>/dev/null || echo "none")
            if [ "$sdv" != "none" ] && [ -n "$sdv" ]; then
                virt="vm"
            elif grep -q "hypervisor" /proc/cpuinfo 2>/dev/null; then
                virt="vm"
            else
                virt="none"
            fi
            ;;
        *) virt="none" ;;
    esac

    # Pick profile
    if [ "$virt" = "vm" ]; then
        echo "vm"
    elif [ "$battery" = "yes" ]; then
        echo "laptop"
    elif [ "$ram_gb" -ge 32 ] 2>/dev/null; then
        echo "server"
    else
        echo "laptop"
    fi
}

# ── Apply env override (portable, không dùng sed -i) ──────
# Sửa hoặc thêm KEY=VALUE vào file .env
apply_override() {
    local key="$1" value="$2" file="$3"
    local tmp="${file}.tmp"
    grep -v "^${key}=" "$file" > "$tmp" 2>/dev/null || cp "$file" "$tmp"
    echo "${key}=${value}" >> "$tmp"
    mv "$tmp" "$file"
}

# ── Parse flags ──────────────────────────────────────────
USE_PYTHON=false
HW_PROFILE=""
OVR_PARTITIONS=""
OVR_KAFKA_HEAP=""
OVR_SPARK_WORKERS=""
OVR_SPARK_MEMORY=""
OVR_SPARK_CORES=""

for arg in "$@"; do
    case "$arg" in
        --python)          USE_PYTHON=true ;;
        --profile=*)       HW_PROFILE="${arg#--profile=}" ;;
        --partitions=*)    OVR_PARTITIONS="${arg#--partitions=}" ;;
        --kafka-heap=*)    OVR_KAFKA_HEAP="${arg#--kafka-heap=}" ;;
        --spark-workers=*) OVR_SPARK_WORKERS="${arg#--spark-workers=}" ;;
        --spark-memory=*)  OVR_SPARK_MEMORY="${arg#--spark-memory=}" ;;
        --spark-cores=*)   OVR_SPARK_CORES="${arg#--spark-cores=}" ;;
        --detect)
            _profile=$(auto_detect_profile)
            _os=$(uname -s 2>/dev/null || echo "unknown")
            _ram=0
            case "$_os" in
                MINGW*|MSYS*|CYGWIN*)
                    _bytes=$(wmic computersystem get TotalPhysicalMemory /value 2>/dev/null \
                        | tr -d '\r' | grep "^TotalPhysicalMemory=" | cut -d= -f2 | xargs)
                    [[ "$_bytes" =~ ^[0-9]+$ ]] && _ram=$(( _bytes / 1024 / 1024 / 1024 )) ;;
                Linux*)
                    _mb=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "0")
                    _ram=$(( _mb / 1024 )) ;;
            esac
            echo ""
            echo "  RAM detected : ${_ram}GB"
            echo "  Profile      : $_profile"
            echo ""
            echo "  Profiles:"
            echo "    laptop  — 12-16GB RAM, Spark 3 workers × 2g"
            echo "    server  — 32GB+ RAM,   Spark 6 workers × 4g"
            echo "    vm      — cloud/VM,    Spark 6 workers × 4g"
            echo ""
            echo "  Khởi động: bash start.sh --profile=$_profile"
            echo ""
            exit 0 ;;
        --help|-h)
            echo "Usage: bash start.sh [OPTIONS]"
            echo ""
            echo "Chế độ job:"
            echo "  (mặc định)             Scala JAR (nhanh hơn)"
            echo "  --python               PySpark (jobs/python/cdc_pipeline.py)"
            echo ""
            echo "Profile phần cứng:"
            echo "  --profile=NAME         laptop | server | vm"
            echo "  --detect               Hiển thị hardware info + profile gợi ý"
            echo "  (không truyền)         Tự động detect từ phần cứng"
            echo ""
            echo "Override tham số (áp dụng sau khi load profile):"
            echo "  --partitions=N         Số Kafka partitions"
            echo "  --kafka-heap=Xg        Kafka broker heap (vd: 2g)"
            echo "  --spark-workers=N      Số Spark workers"
            echo "  --spark-memory=Xg      Memory mỗi Spark worker (vd: 4g)"
            echo "  --spark-cores=N        CPU cores mỗi Spark worker"
            echo ""
            echo "Ví dụ:"
            echo "  bash start.sh"
            echo "  bash start.sh --detect"
            echo "  bash start.sh --profile=server"
            echo "  bash start.sh --profile=laptop --partitions=3 --spark-memory=6g"
            echo "  bash start.sh --python --spark-cores=3"
            exit 0 ;;
        *)
            echo "Flag không nhận ra: $arg  (dùng --help để xem usage)"
            exit 1 ;;
    esac
done

# ── Load profile ──────────────────────────────────────────
if [ -z "$HW_PROFILE" ]; then
    HW_PROFILE=$(auto_detect_profile)
    echo "🔍 Auto-detect profile: ${HW_PROFILE}  (override bằng --profile=X)"
fi

PROFILE_FILE="$COMPOSE_DIR/.env.$HW_PROFILE"
if [ ! -f "$PROFILE_FILE" ]; then
    echo "Lỗi: Không tìm thấy profile '$PROFILE_FILE'"
    echo "Profile có sẵn: laptop, server, vm"
    exit 1
fi
cp "$PROFILE_FILE" "$COMPOSE_DIR/.env"
echo "📂 Profile: $HW_PROFILE"

# ── Apply overrides ───────────────────────────────────────
if [ -n "$OVR_PARTITIONS" ]; then
    apply_override "KAFKA_NUM_PARTITIONS" "$OVR_PARTITIONS" "$COMPOSE_DIR/.env"
    echo "  ↳ KAFKA_NUM_PARTITIONS=$OVR_PARTITIONS"
fi
if [ -n "$OVR_KAFKA_HEAP" ]; then
    # Tính Xms = Xmx/2, vd 2g → -Xmx2g -Xms1g
    local_num="${OVR_KAFKA_HEAP%g}"
    local_xms=$(( local_num / 2 ))
    [ "$local_xms" -lt 1 ] && local_xms=1
    apply_override "KAFKA_HEAP_OPTS" "-Xmx${OVR_KAFKA_HEAP} -Xms${local_xms}g" "$COMPOSE_DIR/.env"
    echo "  ↳ KAFKA_HEAP_OPTS=-Xmx${OVR_KAFKA_HEAP} -Xms${local_xms}g"
fi
if [ -n "$OVR_SPARK_WORKERS" ]; then
    apply_override "SPARK_WORKER_COUNT" "$OVR_SPARK_WORKERS" "$COMPOSE_DIR/.env"
    echo "  ↳ SPARK_WORKER_COUNT=$OVR_SPARK_WORKERS"
fi
if [ -n "$OVR_SPARK_MEMORY" ]; then
    apply_override "SPARK_WORKER_MEMORY" "$OVR_SPARK_MEMORY" "$COMPOSE_DIR/.env"
    echo "  ↳ SPARK_WORKER_MEMORY=$OVR_SPARK_MEMORY"
fi
if [ -n "$OVR_SPARK_CORES" ]; then
    apply_override "SPARK_WORKER_CORES" "$OVR_SPARK_CORES" "$COMPOSE_DIR/.env"
    echo "  ↳ SPARK_WORKER_CORES=$OVR_SPARK_CORES"
fi

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; }
info() { echo -e "${CYAN}[→]${NC} $1"; }

echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  CDC Pipeline — Full Startup${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""

DASHBOARD_JSON="$PROJECT_DIR/monitoring/grafana/dashboards/cdc_dashboard.json"

# ============================================================
# 0. Dọn dẹp processes cũ trên host
# ============================================================

info "Dọn dẹp processes cũ..."

pkill -f "spark-submit.*cdc_pipeline" 2>/dev/null || true
sleep 1

# ============================================================
# 1. Khởi động Docker Compose
# ============================================================

info "Khởi động Docker Compose..."
cd "$COMPOSE_DIR"

RUNNING=$(docker compose ps --status running -q 2>/dev/null | wc -l)

if [ "$RUNNING" -gt 0 ]; then
    warn "Phát hiện $RUNNING containers đang chạy"
    info "Đảm bảo tất cả services đang chạy..."
    docker compose up -d 2>&1 | tail -5
    info "Restart Spark cluster để giải phóng resource..."
    docker compose restart spark-master spark-worker-1 spark-worker-2 spark-worker-3 2>&1 | tail -3
    sleep 10
else
    info "Cold start — khởi động tất cả services..."
    docker compose up -d 2>&1 | tail -5

    # ── Auto-detect Kafka Cluster ID conflict ──
    sleep 8
    KAFKA_STATUS=$(docker inspect --format='{{.State.Status}}' cdc-kafka 2>/dev/null || echo "")
    if [ "$KAFKA_STATUS" = "exited" ]; then
        ERR_COUNT=$(docker logs cdc-kafka 2>&1 | grep -c "InconsistentClusterIdException" || true)
        if [ "$ERR_COUNT" -gt 0 ]; then
            warn "Phát hiện Kafka Cluster ID conflict — auto-fix..."
            docker compose down 2>&1 | tail -2
            docker volume rm pipeline_kafka_data pipeline_zookeeper_data 2>/dev/null || true
            log "Đã xóa kafka_data + zookeeper_data volumes"
            info "Khởi động lại..."
            docker compose up -d 2>&1 | tail -5
            sleep 10
        fi
    fi
fi

# ============================================================
# 2. Đợi services healthy
# ============================================================

info "Đợi services khởi động..."

wait_for_container() {
    local name=$1
    local max_wait=$2
    local elapsed=0

    printf "  Đợi %-25s" "$name..."
    while [ $elapsed -lt $max_wait ]; do
        local status
        status=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' "$name" 2>/dev/null || echo "not_found")
        if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
            echo -e " ${GREEN}OK${NC} (${elapsed}s)"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    echo -e " ${YELLOW}TIMEOUT${NC} (${max_wait}s)"
    return 0
}

wait_for_container "cdc-zookeeper" 60
wait_for_container "cdc-kafka" 90
wait_for_container "cdc-mysql" 60
wait_for_container "cdc-mongodb" 60
wait_for_container "cdc-redis" 30
wait_for_container "cdc-debezium" 120
wait_for_container "cdc-spark-master" 60
echo ""

# ============================================================
# 3. Init MySQL (idempotent)
# ============================================================

info "Khởi tạo MySQL database + dữ liệu mẫu..."

docker exec -i cdc-mysql mysql -uroot -proot 2>/dev/null <<'SQL'
CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

CREATE TABLE IF NOT EXISTS customers (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100) NOT NULL,
  phone VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer_id INT NOT NULL,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  total_amount DECIMAL(12,2) NOT NULL,
  status ENUM('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED') DEFAULT 'PENDING',
  FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

REPLACE INTO customers (id, name, email, phone) VALUES
(1, 'Nguyen Van A', 'a@test.com', '0901234567'),
(2, 'Tran Thi B', 'b@test.com', '0912345678'),
(3, 'Le Van C', 'c@test.com', '0923456789');

REPLACE INTO orders (id, customer_id, total_amount, status) VALUES
(1, 1, 150.00, 'PENDING'),
(2, 2, 250.50, 'PROCESSING'),
(3, 1, 99.99, 'SHIPPED');
SQL

log "MySQL sẵn sàng"

# ============================================================
# 4. Đăng ký Debezium connector
# ============================================================

info "Đăng ký Debezium connector..."

for i in $(seq 1 40); do
    curl -sf http://localhost:8083/connectors > /dev/null 2>&1 && break
    [ $i -eq 40 ] && { err "Debezium API không phản hồi"; exit 1; }
    sleep 3
done

EXISTING=$(curl -sf http://localhost:8083/connectors 2>/dev/null || echo "[]")

if echo "$EXISTING" | grep -q "mysql-inventory-connector"; then
    CONN_STATE=$(curl -sf http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null | \
        $PYTHON -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
    if [ "$CONN_STATE" = "RUNNING" ]; then
        log "Connector đang RUNNING — bỏ qua"
    else
        warn "Connector trạng thái $CONN_STATE — tạo lại"
        curl -sf -X DELETE http://localhost:8083/connectors/mysql-inventory-connector > /dev/null 2>&1
        sleep 3
        curl -sf -X POST http://localhost:8083/connectors \
            -H "Content-Type: application/json" \
            -d @"$PROJECT_DIR/demo/config/connector.json" > /dev/null 2>&1 || true
        log "Connector đã tạo lại"
    fi
else
    curl -sf -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @"$PROJECT_DIR/demo/config/connector.json" > /dev/null 2>&1 || true
    log "Connector đã đăng ký mới"
fi

sleep 5

# ============================================================
# 5. Đợi Kafka topics
# ============================================================

info "Đợi Kafka topics..."

for i in $(seq 1 40); do
    TOPICS=$(docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null)
    HC=$(echo "$TOPICS" | grep -c "inventory.inventory.customers" || true)
    HO=$(echo "$TOPICS" | grep -c "inventory.inventory.orders" || true)
    if [ "$HC" -gt 0 ] && [ "$HO" -gt 0 ]; then
        log "Kafka topics sẵn sàng: customers ✓  orders ✓"
        break
    fi
    if [ $i -eq 40 ]; then
        warn "Kafka topics timeout — Debezium có thể vẫn đang khởi động. Tiếp tục..."
        break
    fi
    sleep 3
done

# ============================================================
# 6. Kill Spark cũ + Xóa checkpoint + Submit
# ============================================================

info "Chuẩn bị Spark job..."

docker exec cdc-spark-master bash -c '
    PIDS=$(ps aux | grep "[s]park-submit" | awk "{print \$2}")
    [ -n "$PIDS" ] && echo "$PIDS" | xargs kill 2>/dev/null && sleep 5
' 2>/dev/null || true

ACTIVE_APPS=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
    $PYTHON -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")

if [ "$ACTIVE_APPS" -gt 0 ]; then
    warn "Còn $ACTIVE_APPS app(s) chiếm resource — restart Spark cluster..."
    cd "$COMPOSE_DIR"
    docker compose restart spark-master spark-worker-1 spark-worker-2 spark-worker-3 2>&1 | tail -3
    sleep 20
    for i in $(seq 1 20); do
        curl -sf http://localhost:8080 > /dev/null 2>&1 && break
        sleep 3
    done
    log "Spark cluster restart xong"
fi

docker exec --user root cdc-spark-master chmod 777 /tmp/spark-checkpoint 2>/dev/null || true
docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint/cdc-pipeline 2>/dev/null || true
log "Đã xóa Spark checkpoint"

if [ "$USE_PYTHON" = true ]; then
    info "Submit Spark job (PYTHON mode)..."
    docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
        --master spark://cdc-spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
        /opt/spark/jobs/python/cdc_pipeline.py
    log "Spark job submitted (Python)"
else
    info "Submit Spark job (SCALA mode — mặc định)..."
    docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
        --class CdcRedisConsumer \
        --master spark://cdc-spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0 \
        /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar
    log "Spark job submitted (Scala JAR)"
fi

# ============================================================
# 7. Đợi Spark xử lý batch đầu tiên
# ============================================================

info "Đợi Spark xử lý data (tối đa 180s)..."

SPARK_OK=false
for i in $(seq 1 60); do
    MC=$(docker exec cdc-mongodb mongosh --quiet --eval \
        "db.getSiblingDB('inventory').customers.countDocuments()" 2>/dev/null || echo "0")
    if [ "$MC" -gt 0 ] 2>/dev/null; then
        log "Spark xử lý thành công — MongoDB: $MC customers"
        SPARK_OK=true
        # Đợi thêm để executor register xong với master
        sleep 10
        break
    fi
    if [ $((i % 10)) -eq 0 ]; then
        APPS=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
            $PYTHON -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")
        [ "$APPS" -eq 0 ] && warn "Spark app có thể đã crash"
    fi
    sleep 3
done
[ "$SPARK_OK" = false ] && warn "Spark chưa xong — có thể cần thêm thời gian"

# ============================================================
# 8. Kiểm tra Metrics Exporter (Đã Dockerize)
# ============================================================

info "Kiểm tra metrics exporter..."
cd "$PROJECT_DIR"

wait_for_container "cdc-metrics-exporter" 30

if curl -sf http://localhost:8000/metrics > /dev/null 2>&1; then
    log "Metrics exporter đang chạy thành công (trên Docker)"
else
    warn "Metrics exporter chưa phản hồi ở localhost:8000"
fi

# ============================================================
# 9. Auto-patch Grafana datasource UID trong dashboard
# ============================================================

info "Cập nhật Grafana datasource UID..."

# Đợi Grafana API sẵn sàng
for i in $(seq 1 20); do
    curl -sf -u admin:admin http://localhost:3000/api/health > /dev/null 2>&1 && break
    sleep 3
done

# Restart Grafana để force reload provisioning (fix UID mismatch sau stop -v)
cd "$COMPOSE_DIR"
docker compose restart grafana 2>&1 | tail -1

# Đợi Grafana khởi động lại
for i in $(seq 1 20); do
    curl -sf -u admin:admin http://localhost:3000/api/health > /dev/null 2>&1 && break
    sleep 3
done
log "Grafana provisioning reload OK"
cd "$PROJECT_DIR"

# ============================================================
# 10. Báo cáo trạng thái
# ============================================================

echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  Trạng thái Pipeline${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""

MC=$(docker exec cdc-mysql mysql -uroot -proot -N -e "SELECT COUNT(*) FROM inventory.customers" 2>/dev/null || echo "?")
MO=$(docker exec cdc-mysql mysql -uroot -proot -N -e "SELECT COUNT(*) FROM inventory.orders" 2>/dev/null || echo "?")
printf "  %-18s customers=%-4s orders=%-4s\n" "MySQL:" "$MC" "$MO"

DS=$(curl -sf http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null | \
    $PYTHON -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "?")
printf "  %-18s %s\n" "Debezium:" "$DS"

KT=$(docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -c "inventory.inventory" || echo "?")
printf "  %-18s %s CDC topics\n" "Kafka:" "$KT"

SA=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
    $PYTHON -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "?")
printf "  %-18s %s active app(s)\n" "Spark:" "$SA"

GC=$(docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').customers.countDocuments()" 2>/dev/null || echo "?")
GO=$(docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').orders.countDocuments()" 2>/dev/null || echo "?")
printf "  %-18s customers=%-4s orders=%-4s\n" "MongoDB:" "$GC" "$GO"

RK=$(docker exec cdc-redis redis-cli dbsize 2>/dev/null | awk '{print $1}' || echo "?")
printf "  %-18s %s keys\n" "Redis:" "$RK"

PH=$(curl -sf http://localhost:9090/api/v1/targets 2>/dev/null | \
    $PYTHON -c "import sys,json; t=json.load(sys.stdin)['data']['activeTargets']; print(t[0]['health'] if t else '?')" 2>/dev/null || echo "?")
printf "  %-18s target %s\n" "Prometheus:" "$PH"

GH=$(curl -sf http://localhost:3000/api/health 2>/dev/null | \
    $PYTHON -c "import sys,json; print(json.load(sys.stdin).get('database','?'))" 2>/dev/null || echo "?")
printf "  %-18s %s\n" "Grafana:" "$GH"

echo ""
if [ "$MC" = "$GC" ] && [ "$MO" = "$GO" ] 2>/dev/null; then
    echo -e "  ${GREEN}★ MySQL ↔ MongoDB: IN SYNC${NC}"
else
    echo -e "  ${YELLOW}⚠ MySQL ↔ MongoDB: ĐANG SYNC (đợi thêm)${NC}"
fi

echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  URLs${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""
echo "  Grafana:       http://localhost:3000  (admin/admin)"
echo "  Spark Master:  http://localhost:8080"
echo "  Kafka UI:      http://localhost:8090"
echo "  Prometheus:    http://localhost:9090"
echo "  Debezium API:  http://localhost:8083"
echo "  Metrics:       http://localhost:8000/metrics"
echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "  ${GREEN}✓ Pipeline đã sẵn sàng!${NC}"
echo -e "  ${CYAN}Dừng: bash stop.sh${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""
