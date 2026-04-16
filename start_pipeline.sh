#!/bin/bash
# ============================================================
# start_pipeline.sh — Khởi động CDC Pipeline hoàn chỉnh
#
# Script này làm đúng thứ tự:
#   1. Kiểm tra Docker services healthy
#   2. Đợi Kafka topics sẵn sàng (fix lỗi khi mới bật máy)
#   3. Kiểm tra/register Debezium connector
#   4. Submit Spark job (Kafka → MongoDB + Redis)
#   5. Xác nhận pipeline end-to-end đang chạy
#
# Cách dùng:
#   chmod +x start_pipeline.sh
#   ./start_pipeline.sh
#   ./start_pipeline.sh --skip-spark   # chỉ check services, không submit Spark
# ============================================================

set -euo pipefail

# ── Config ──────────────────────────────────────────────────
JAR_PATH="/opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar"
CHECKPOINT_DIR="/tmp/spark-checkpoint/cdc-pipeline"
KAFKA_TOPICS="inventory.inventory.customers,inventory.inventory.orders"
CONNECTOR_NAME="mysql-inventory-connector"
CONNECTOR_CONFIG_FILE="$(dirname "$0")/demo/connector.json"

# Màu
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

SKIP_SPARK=false
for arg in "$@"; do
  [[ "$arg" == "--skip-spark" ]] && SKIP_SPARK=true
done

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*"; }
info() { echo -e "${DIM}  $*${RESET}"; }
step() { echo -e "\n${BOLD}── $* ──${RESET}"; }

# ── Banner ───────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║     CDC PIPELINE — START SCRIPT                     ║"
echo "║     MySQL → Debezium → Kafka → Spark → MongoDB+Redis║"
echo "╚══════════════════════════════════════════════════════╝"
echo -e "${RESET}"

# ════════════════════════════════════════════════════════════
# STEP 1: Docker services
# ════════════════════════════════════════════════════════════
step "STEP 1: Kiểm tra Docker services"

services=("cdc-mysql" "cdc-kafka" "cdc-debezium" "cdc-mongodb" "cdc-redis" "cdc-spark-master")
all_ok=true
for svc in "${services[@]}"; do
  status=$(docker inspect --format='{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
  health=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "none")
  if [[ "$status" == "running" ]]; then
    if [[ "$health" == "healthy" || "$health" == "none" ]]; then
      ok "$svc ($health)"
    else
      warn "$svc running nhưng health=$health (đợi thêm...)"
      all_ok=false
    fi
  else
    err "$svc: $status"
    all_ok=false
  fi
done

if [[ "$all_ok" == "false" ]]; then
  warn "Một số service chưa healthy. Đợi 15s rồi tiếp tục..."
  sleep 15
fi

# ════════════════════════════════════════════════════════════
# STEP 2: Đợi Kafka topics sẵn sàng
# ════════════════════════════════════════════════════════════
step "STEP 2: Kiểm tra Kafka topics"

REQUIRED_TOPICS=(
  "inventory.inventory.customers"
  "inventory.inventory.orders"
)

max_wait=60
waited=0
while true; do
  existing_topics=$(docker exec cdc-kafka \
    kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")

  missing=()
  for topic in "${REQUIRED_TOPICS[@]}"; do
    if ! echo "$existing_topics" | grep -q "^${topic}$"; then
      missing+=("$topic")
    fi
  done

  if [[ ${#missing[@]} -eq 0 ]]; then
    ok "Tất cả Kafka topics sẵn sàng"
    for t in "${REQUIRED_TOPICS[@]}"; do
      info "topic: $t"
    done
    break
  fi

  if [[ $waited -ge $max_wait ]]; then
    warn "Topics chưa có sau ${max_wait}s: ${missing[*]}"
    warn "Có thể Debezium connector chưa chạy → sang bước 3"
    break
  fi

  info "Chờ topics: ${missing[*]} (${waited}s / ${max_wait}s)..."
  sleep 5
  waited=$((waited + 5))
done

# ════════════════════════════════════════════════════════════
# STEP 3: Debezium connector
# ════════════════════════════════════════════════════════════
step "STEP 3: Kiểm tra Debezium connector"

connector_status=$(curl -s \
  "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" \
  2>/dev/null || echo "NOT_FOUND")

if [[ "$connector_status" == "RUNNING" ]]; then
  ok "Debezium connector RUNNING"

elif [[ "$connector_status" == "FAILED" ]]; then
  warn "Connector FAILED — thử restart..."
  curl -s -X POST \
    "http://localhost:8083/connectors/${CONNECTOR_NAME}/restart" \
    -H "Content-Type: application/json" > /dev/null 2>&1
  sleep 5
  connector_status=$(curl -s \
    "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" \
    2>/dev/null || echo "UNKNOWN")
  if [[ "$connector_status" == "RUNNING" ]]; then
    ok "Connector restarted → RUNNING"
  else
    err "Connector vẫn $connector_status sau restart"
    info "Thử register lại: curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @demo/connector.json"
  fi

elif [[ "$connector_status" == "NOT_FOUND" ]]; then
  warn "Connector chưa được register"
  if [[ -f "$CONNECTOR_CONFIG_FILE" ]]; then
    info "Tìm thấy $CONNECTOR_CONFIG_FILE — đang register..."
    result=$(curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @"$CONNECTOR_CONFIG_FILE" 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name','error'))" \
      2>/dev/null || echo "error")
    if [[ "$result" == "$CONNECTOR_NAME" || "$result" != "error" ]]; then
      ok "Connector registered: $result"
      sleep 5
    else
      err "Register thất bại. Chạy tay: bash demo/register-connector.sh"
    fi
  else
    err "Không tìm thấy connector.json"
    info "Chạy tay: bash demo/register-connector.sh"
  fi

else
  warn "Connector status: $connector_status"
fi

# ════════════════════════════════════════════════════════════
# STEP 4: Submit Spark job
# ════════════════════════════════════════════════════════════
step "STEP 4: Submit Spark Streaming job"

if [[ "$SKIP_SPARK" == "true" ]]; then
  warn "Skip Spark (--skip-spark flag)"
else
  # Kiểm tra job đã chạy chưa
  active_apps=$(curl -s http://localhost:8080/json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('activeapps',[])))" \
    2>/dev/null || echo "0")

  if [[ "$active_apps" -gt 0 ]]; then
    ok "Spark đã có $active_apps active app — skip submit"
    info "Nếu muốn restart: dừng app cũ trên http://localhost:8080 rồi chạy lại script"
  else
    info "Submit Spark job vào container spark-master..."
    info "JAR: $JAR_PATH"
    info "Topics: $KAFKA_TOPICS"
    info "Checkpoint: $CHECKPOINT_DIR"

    # Xóa checkpoint cũ để tránh conflict offset
    docker exec cdc-spark-master rm -rf "$CHECKPOINT_DIR" 2>/dev/null || true

    # Submit trong background (streaming job chạy mãi)
    docker exec -d cdc-spark-master \
      /opt/spark/bin/spark-submit \
        --master spark://cdc-spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        --conf "spark.sql.streaming.checkpointLocation=${CHECKPOINT_DIR}" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        "$JAR_PATH" 2>/dev/null

    info "Job submitted (background). Đợi 10s để job start..."
    sleep 10

    # Xác nhận
    active_apps=$(curl -s http://localhost:8080/json 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('activeapps',[])))" \
      2>/dev/null || echo "0")

    if [[ "$active_apps" -gt 0 ]]; then
      ok "Spark job RUNNING ($active_apps active app)"
      info "Xem chi tiết: http://localhost:8080"
      info "Streaming UI:  http://localhost:4040"
    else
      warn "Job chưa thấy trong active apps"
      info "Kiểm tra logs: docker logs cdc-spark-master --tail 50"
      info "Hoặc xem UI: http://localhost:8080"
    fi
  fi
fi

# ════════════════════════════════════════════════════════════
# STEP 5: Xác nhận E2E
# ════════════════════════════════════════════════════════════
step "STEP 5: Xác nhận pipeline end-to-end"

# Test nhanh: insert 1 record, đợi 5s, check MongoDB
info "Insert 1 test record vào MySQL..."
docker exec cdc-mysql mysql -uroot -proot inventory \
  --skip-column-names -s \
  -e "INSERT INTO customers (name, email, phone) VALUES ('PipelineTest', 'pipeline@test.com', '0900000000');" \
  2>/dev/null

info "Đợi 5s cho Spark trigger..."
sleep 5

# Check MongoDB
mongo_count=$(docker exec cdc-mongodb mongosh inventory \
  --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "?")

# Check Redis
redis_count=$(docker exec cdc-redis redis-cli get customers:total 2>/dev/null | tr -d '[:space:]' || echo "?")

mysql_count=$(docker exec cdc-mysql mysql -uroot -proot inventory \
  --skip-column-names -s -e "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]' || echo "?")

echo ""
echo -e "  MySQL customers:   ${CYAN}${mysql_count}${RESET}"
echo -e "  MongoDB customers: ${CYAN}${mongo_count}${RESET}"
echo -e "  Redis customers:total key: ${CYAN}${redis_count}${RESET}"
echo ""

if [[ "$mysql_count" == "$mongo_count" && "$mysql_count" != "?" ]]; then
  ok "MySQL == MongoDB → Pipeline E2E đang hoạt động!"
elif [[ "$mongo_count" == "0" || "$mongo_count" == "?" ]]; then
  warn "MongoDB chưa có data → Spark job có thể chưa kịp process"
  info "Đợi thêm 10s rồi check lại: docker exec cdc-mongodb mongosh inventory --eval 'db.customers.countDocuments()'"
else
  warn "MySQL($mysql_count) ≠ MongoDB($mongo_count) — có thể đang sync..."
fi

# ════════════════════════════════════════════════════════════
# DONE
# ════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Pipeline started. Các URL quan trọng:${RESET}"
echo -e "  Grafana:    ${CYAN}http://localhost:3000${RESET}  (admin/admin)"
echo -e "  Spark UI:   ${CYAN}http://localhost:8080${RESET}"
echo -e "  Spark Job:  ${CYAN}http://localhost:4040${RESET}  (khi job đang chạy)"
echo -e "  Kafka UI:   ${CYAN}http://localhost:8083/connectors${RESET}"
echo -e "  Prometheus: ${CYAN}http://localhost:9090${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════${RESET}"
echo ""
echo -e "  Chạy demo live: ${CYAN}./demo_live.sh${RESET}"
echo -e "  Đo throughput:  ${CYAN}./demo_live.sh --tps${RESET}"
echo ""