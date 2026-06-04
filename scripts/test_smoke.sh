#!/bin/bash
# ============================================================
# CDC Pipeline — Smoke Test Suite
# Chạy sau khi pipeline khởi động để kiểm tra toàn bộ components
#
# Usage:
#   bash test_smoke.sh            # kiểm tra toàn bộ
#   bash test_smoke.sh --quick    # chỉ kiểm tra connectivity
#
# Exit code: 0 = tất cả pass, 1 = có test fail
# ============================================================

set -euo pipefail

export MSYS_NO_PATHCONV=1

# Dùng --version để test thật, tránh Windows Store stub (command -v tìm thấy stub nhưng không chạy được)
if python3 --version >/dev/null 2>&1; then
    PYTHON=python3
elif python --version >/dev/null 2>&1; then
    PYTHON=python
else
    PYTHON=python3
fi

QUICK=false
for arg in "$@"; do
    [ "$arg" = "--quick" ] && QUICK=true
done

# Colors
G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'
B='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; SKIP=0

pass() { echo -e "${G}  [PASS]${NC} $1"; PASS=$((PASS+1)); }
fail() { echo -e "${R}  [FAIL]${NC} $1"; FAIL=$((FAIL+1)); }
skip() { echo -e "${Y}  [SKIP]${NC} $1"; SKIP=$((SKIP+1)); }
section() { echo -e "\n${BOLD}${B}══ $1 ══${NC}"; }

# ── 1. CONTAINERS ─────────────────────────────────────────
section "1. Container Status"

REQUIRED_CONTAINERS=(
    "cdc-zookeeper"
    "cdc-kafka"
    "cdc-mysql"
    "cdc-mongodb"
    "cdc-redis"
    "cdc-debezium"
    "cdc-spark-master"
    "cdc-spark-worker-1"
    "cdc-spark-worker-2"
    "cdc-spark-worker-3"
    "cdc-metrics-exporter"
    "cdc-prometheus"
    "cdc-grafana"
)

for c in "${REQUIRED_CONTAINERS[@]}"; do
    STATUS=$(docker inspect --format='{{.State.Status}}' "$c" 2>/dev/null || echo "not_found")
    if [ "$STATUS" = "running" ]; then
        pass "$c running"
    else
        fail "$c — status: $STATUS"
    fi
done

# ── 2. PORT CONNECTIVITY ──────────────────────────────────
section "2. Port Connectivity"

check_port() {
    local name=$1; local port=$2; local proto=${3:-tcp}
    if [ "$proto" = "http" ]; then
        if curl -sf --max-time 3 "http://localhost:$port" > /dev/null 2>&1; then
            pass "$name (localhost:$port)"
        else
            fail "$name (localhost:$port) — không kết nối được"
        fi
    else
        # TCP check qua docker exec (tránh phụ thuộc nc trên Windows)
        if docker exec cdc-mysql bash -c "timeout 1 bash -c 'echo >/dev/tcp/host.docker.internal/$port'" 2>/dev/null || \
           (command -v nc > /dev/null && nc -z -w3 localhost "$port" 2>/dev/null); then
            pass "$name (localhost:$port)"
        else
            # Fallback: kiểm tra container expose port
            EXPOSED=$(docker inspect --format='{{range $p, $conf := .NetworkSettings.Ports}}{{$p}} {{end}}' \
                "$(docker ps --filter "publish=$port" --format "{{.Names}}" 2>/dev/null | head -1)" 2>/dev/null || echo "")
            if echo "$EXPOSED" | grep -q "$port"; then
                pass "$name (localhost:$port) — port mapped"
            else
                fail "$name (localhost:$port) — không kết nối được"
            fi
        fi
    fi
}

check_port_http() {
    local name=$1; local port=$2; local path=${3:-}
    if curl -sf --max-time 3 "http://localhost:$port$path" > /dev/null 2>&1; then
        pass "$name (localhost:$port)"
    else
        fail "$name (localhost:$port) — không kết nối được"
    fi
}

check_port "MySQL"          3306 tcp
check_port "Kafka"          9092 tcp
check_port_http "Debezium API"   8083 "/connectors"
check_port_http "Spark Master"   8080
check_port_http "Metrics Exporter" 8000 "/metrics"
check_port_http "Prometheus"     9090
check_port_http "Grafana"        3000
check_port "MongoDB"        27017 tcp
check_port "Redis"          6379 tcp

if [ "$QUICK" = true ]; then
    echo ""
    echo -e "${Y}[Quick mode] Bỏ qua các test data và pipeline${NC}"
else

# ── 3. DEBEZIUM CONNECTOR ─────────────────────────────────
section "3. Debezium Connector"

CONN_STATUS=$(curl -sf http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null | \
    $PYTHON -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "ERROR")

if [ "$CONN_STATUS" = "RUNNING" ]; then
    pass "Debezium connector RUNNING"
else
    fail "Debezium connector — state: $CONN_STATUS"
fi

TASK_STATUS=$(curl -sf http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null | \
    $PYTHON -c "import sys,json; d=json.load(sys.stdin); t=d.get('tasks',[{}]); print(t[0].get('state','?') if t else '?')" 2>/dev/null || echo "ERROR")

if [ "$TASK_STATUS" = "RUNNING" ]; then
    pass "Debezium task RUNNING"
else
    fail "Debezium task — state: $TASK_STATUS"
fi

# ── 4. KAFKA TOPICS ───────────────────────────────────────
section "4. Kafka Topics"

TOPICS=$(docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || true)

if echo "$TOPICS" | grep -q "inventory.inventory.customers"; then
    pass "Topic: inventory.inventory.customers"
else
    fail "Topic: inventory.inventory.customers — không tồn tại"
fi

if echo "$TOPICS" | grep -q "inventory.inventory.orders"; then
    pass "Topic: inventory.inventory.orders"
else
    fail "Topic: inventory.inventory.orders — không tồn tại"
fi

# ── 5. SPARK ──────────────────────────────────────────────
section "5. Spark"

ACTIVE_APPS=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
    $PYTHON -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")

if [ "$ACTIVE_APPS" -gt 0 ] 2>/dev/null; then
    pass "Spark: $ACTIVE_APPS active app(s)"
else
    fail "Spark: không có app nào đang chạy"
fi

WORKERS=$(curl -sf http://localhost:8080/json/ 2>/dev/null | \
    $PYTHON -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers', 0))" 2>/dev/null || echo "0")

if [ "$WORKERS" -ge 1 ] 2>/dev/null; then
    pass "Spark workers alive: $WORKERS"
else
    fail "Spark: không có worker nào alive"
fi

# ── 6. DATA SYNC CHECK ────────────────────────────────────
section "6. Data Sync (MySQL ↔ MongoDB)"

MYSQL_C=$(docker exec cdc-mysql mysql -uroot -proot -N \
    -e "SELECT COUNT(*) FROM inventory.customers" 2>/dev/null | tr -d '\r\n' || echo "0")
MONGO_C=$(docker exec cdc-mongodb mongosh --quiet \
    --eval "db.getSiblingDB('inventory').customers.countDocuments()" 2>/dev/null | tr -d '\r\n' || echo "0")

if [ "$MYSQL_C" -gt 0 ] 2>/dev/null; then
    pass "MySQL customers: $MYSQL_C records"
else
    fail "MySQL customers: 0 records (pipeline chưa chạy?)"
fi

if [ "$MONGO_C" -gt 0 ] 2>/dev/null; then
    pass "MongoDB customers: $MONGO_C documents"
else
    fail "MongoDB customers: 0 documents (Spark chưa sync?)"
fi

if [ "$MYSQL_C" = "$MONGO_C" ] && [ "$MYSQL_C" -gt 0 ] 2>/dev/null; then
    pass "MySQL ↔ MongoDB IN SYNC ($MYSQL_C)"
else
    fail "MySQL ($MYSQL_C) ≠ MongoDB ($MONGO_C) — đang lag hoặc chưa sync"
fi

REDIS_KEYS=$(docker exec cdc-redis redis-cli dbsize 2>/dev/null | awk '{print $1}' || echo "0")
if [ "$REDIS_KEYS" -gt 0 ] 2>/dev/null; then
    pass "Redis: $REDIS_KEYS keys"
else
    fail "Redis: 0 keys"
fi

# ── 7. E2E INSERT TEST ────────────────────────────────────
section "7. E2E Insert → Sync Test"

echo "  Inserting 1 test record vào MySQL..."
TEST_ID=9999999
docker exec cdc-mysql mysql -uroot -proot -e \
    "INSERT INTO inventory.customers (id,name,email,phone) VALUES ($TEST_ID,'SmokeTest','smoke@test.com','0900000000') ON DUPLICATE KEY UPDATE name='SmokeTest'" \
    2>/dev/null || true

echo "  Đợi pipeline sync (tối đa 30s)..."
SYNCED=false
for i in $(seq 1 10); do
    sleep 3
    FOUND=$(docker exec cdc-mongodb mongosh --quiet \
        --eval "db.getSiblingDB('inventory').customers.countDocuments({_id: $TEST_ID})" 2>/dev/null || echo "0")
    if [ "$FOUND" -gt 0 ] 2>/dev/null; then
        pass "E2E sync: record $TEST_ID xuất hiện trong MongoDB sau $((i*3))s"
        SYNCED=true
        break
    fi
done
[ "$SYNCED" = false ] && fail "E2E sync: record $TEST_ID không đến MongoDB trong 30s"

# Cleanup test record
docker exec cdc-mysql mysql -uroot -proot -e \
    "DELETE FROM inventory.customers WHERE id=$TEST_ID" 2>/dev/null || true

# ── 8. METRICS EXPORTER ───────────────────────────────────
section "8. Metrics Exporter"

METRICS=$(curl -sf http://localhost:8000/metrics 2>/dev/null || echo "")

check_metric() {
    local m=$1
    if echo "$METRICS" | grep -q "^$m"; then
        pass "Metric present: $m"
    else
        fail "Metric missing: $m"
    fi
}

check_metric "cdc_mysql_customers_total"
check_metric "cdc_mongo_customers_total"
check_metric "cdc_kafka_customers_offset"
check_metric "cdc_mysql_insert_rate"
check_metric "cdc_mongo_write_rate"
check_metric "cdc_lag_total"

# ── 9. PROMETHEUS SCRAPE ──────────────────────────────────
section "9. Prometheus"

PROM_HEALTH=$(curl -sf 'http://localhost:9090/api/v1/query?query=up' 2>/dev/null | \
    $PYTHON -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
if results:
    print('up')
else:
    print('no_data')
" 2>/dev/null || echo "error")

if [ "$PROM_HEALTH" = "up" ]; then
    pass "Prometheus: đang scrape metrics"
else
    # Fallback: kiểm tra xem có metric nào không
    METRIC_COUNT=$(curl -sf 'http://localhost:9090/api/v1/label/__name__/values' 2>/dev/null | \
        $PYTHON -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))" 2>/dev/null || echo "0")
    if [ "$METRIC_COUNT" -gt 0 ] 2>/dev/null; then
        pass "Prometheus: $METRIC_COUNT metrics available"
    else
        fail "Prometheus: không có dữ liệu — $PROM_HEALTH"
    fi
fi

# ── 10. PROFILE FILES ─────────────────────────────────────
section "10. Hardware Profiles"

for p in laptop server vm; do
    if [ -f "config/.env.$p" ]; then
        pass "Profile exists: config/.env.$p"
    else
        fail "Profile missing: config/.env.$p"
    fi
done

fi  # end non-quick tests

# ── SUMMARY ───────────────────────────────────────────────
echo ""
echo -e "${BOLD}══════════════════════════════════════${NC}"
echo -e "${BOLD}  KẾT QUẢ SMOKE TEST${NC}"
echo -e "${BOLD}══════════════════════════════════════${NC}"
echo ""
echo -e "  ${G}PASS: $PASS${NC}"
[ $FAIL -gt 0 ] && echo -e "  ${R}FAIL: $FAIL${NC}" || echo -e "  FAIL: $FAIL"
[ $SKIP -gt 0 ] && echo -e "  ${Y}SKIP: $SKIP${NC}"
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "  ${G}${BOLD}✓ Tất cả tests PASS — pipeline sẵn sàng!${NC}"
    exit 0
else
    echo -e "  ${R}${BOLD}✗ $FAIL test(s) FAIL — kiểm tra lại ở trên${NC}"
    exit 1
fi
