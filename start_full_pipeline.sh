#!/bin/bash
# ============================================================
# CDC Pipeline — Full Startup Script
# Xử lý mọi tình huống: cold start, restart, sau down -v
# Bao gồm auto-patch Grafana datasource UID
# Sử dụng: cd ~/cdc-pipeline && bash start_full_pipeline.sh
# ============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$PROJECT_DIR/pipeline"
DASHBOARD_JSON="$PROJECT_DIR/monitoring/grafana/dashboards/cdc_dashboard.json"

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

# ============================================================
# 0. Dọn dẹp processes cũ trên host
# ============================================================

info "Dọn dẹp processes cũ..."
pkill -f "metrics_exporter.py" 2>/dev/null && log "Đã dừng metrics exporter cũ" || true
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
    info "Restart Spark cluster để giải phóng resource..."
    docker compose restart spark-master spark-worker-1 spark-worker-2 spark-worker-3 2>&1 | tail -3
    sleep 10
else
    info "Cold start — khởi động tất cả services..."
    docker compose up -d 2>&1 | tail -5
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
        python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
    if [ "$CONN_STATE" = "RUNNING" ]; then
        log "Connector đang RUNNING — bỏ qua"
    else
        warn "Connector trạng thái $CONN_STATE — tạo lại"
        curl -sf -X DELETE http://localhost:8083/connectors/mysql-inventory-connector > /dev/null 2>&1
        sleep 3
        curl -sf -X POST http://localhost:8083/connectors \
            -H "Content-Type: application/json" \
            -d @"$PROJECT_DIR/demo/connector.json" > /dev/null 2>&1
        log "Connector đã tạo lại"
    fi
else
    curl -sf -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @"$PROJECT_DIR/demo/connector.json" > /dev/null 2>&1
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
    [ $i -eq 40 ] && { err "Kafka topics timeout"; exit 1; }
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
    python3 -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")

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

docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint/cdc-pipeline 2>/dev/null || true
log "Đã xóa Spark checkpoint"

info "Submit Spark streaming job..."
docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
    --master spark://cdc-spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    /opt/spark/jobs/python/cdc_pipeline.py

log "Spark job submitted"

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
        break
    fi
    if [ $((i % 10)) -eq 0 ]; then
        APPS=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
            python3 -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")
        [ "$APPS" -eq 0 ] && warn "Spark app có thể đã crash"
    fi
    sleep 3
done
[ "$SPARK_OK" = false ] && warn "Spark chưa xong — có thể cần thêm thời gian"

# ============================================================
# 8. Khởi động Metrics Exporter
# ============================================================

info "Khởi động metrics exporter..."
cd "$PROJECT_DIR"

if curl -sf http://localhost:8000/metrics > /dev/null 2>&1; then
    log "Metrics exporter đã chạy sẵn"
else
    nohup python3 metrics_exporter.py > metrics_exporter.log 2>&1 &
    sleep 5
    if curl -sf http://localhost:8000/metrics > /dev/null 2>&1; then
        log "Metrics exporter đang chạy"
    else
        warn "Metrics exporter lỗi — chạy: pip3 install prometheus-client pymysql pymongo redis kafka-python"
    fi
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

# Lấy UID thực tế của Prometheus datasource
ACTUAL_UID=$(curl -sf -u admin:admin http://localhost:3000/api/datasources 2>/dev/null | \
    python3 -c "
import sys, json
ds = json.load(sys.stdin)
for d in ds:
    if d['type'] == 'prometheus':
        print(d['uid'])
        break
" 2>/dev/null || echo "")

if [ -n "$ACTUAL_UID" ] && [ -f "$DASHBOARD_JSON" ]; then
    # Tìm UID cũ trong dashboard JSON (bất kỳ UID nào đang có)
    OLD_UID=$(python3 -c "
import json
data = json.loads(open('$DASHBOARD_JSON').read())
for p in data.get('panels', []):
    ds = p.get('datasource', {})
    if isinstance(ds, dict) and ds.get('uid'):
        print(ds['uid'])
        break
    for t in p.get('targets', []):
        tds = t.get('datasource', {})
        if isinstance(tds, dict) and tds.get('uid'):
            print(tds['uid'])
            break
" 2>/dev/null | head -1)

    if [ -n "$OLD_UID" ] && [ "$OLD_UID" != "$ACTUAL_UID" ]; then
        info "Dashboard UID cũ: $OLD_UID → mới: $ACTUAL_UID"
        sed -i "s/$OLD_UID/$ACTUAL_UID/g" "$DASHBOARD_JSON"
        log "Đã cập nhật dashboard JSON"

        # Restart Grafana để reload dashboard
        cd "$COMPOSE_DIR"
        docker compose restart grafana 2>&1 | tail -1
        sleep 10
        log "Grafana đã restart với UID mới"
    elif [ "$OLD_UID" = "$ACTUAL_UID" ]; then
        log "Dashboard UID đã đúng — không cần sửa"
    else
        warn "Không tìm được UID cũ trong dashboard"
    fi
else
    if [ -z "$ACTUAL_UID" ]; then
        warn "Không lấy được Prometheus datasource UID từ Grafana"
    fi
    if [ ! -f "$DASHBOARD_JSON" ]; then
        warn "Không tìm thấy dashboard JSON: $DASHBOARD_JSON"
    fi
fi

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
    python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "?")
printf "  %-18s %s\n" "Debezium:" "$DS"

KT=$(docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -c "inventory.inventory" || echo "?")
printf "  %-18s %s CDC topics\n" "Kafka:" "$KT"

SA=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
    python3 -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "?")
printf "  %-18s %s active app(s)\n" "Spark:" "$SA"

GC=$(docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').customers.countDocuments()" 2>/dev/null || echo "?")
GO=$(docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').orders.countDocuments()" 2>/dev/null || echo "?")
printf "  %-18s customers=%-4s orders=%-4s\n" "MongoDB:" "$GC" "$GO"

RK=$(docker exec cdc-redis redis-cli dbsize 2>/dev/null | awk '{print $2}' || echo "?")
printf "  %-18s %s keys\n" "Redis:" "$RK"

PH=$(curl -sf http://localhost:9090/api/v1/targets 2>/dev/null | \
    python3 -c "import sys,json; t=json.load(sys.stdin)['data']['activeTargets']; print(t[0]['health'] if t else '?')" 2>/dev/null || echo "?")
printf "  %-18s target %s\n" "Prometheus:" "$PH"

GH=$(curl -sf http://localhost:3000/api/health 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('database','?'))" 2>/dev/null || echo "?")
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
echo "  Prometheus:    http://localhost:9090"
echo "  Debezium API:  http://localhost:8083"
echo "  Metrics:       http://localhost:8000/metrics"
echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "  ${GREEN}✓ Pipeline đã sẵn sàng!${NC}"
echo -e "  ${CYAN}Dừng: bash stop_pipeline.sh${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""
