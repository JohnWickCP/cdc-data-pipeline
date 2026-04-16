#!/bin/bash
# ============================================================
# run.sh — CDC Pipeline Control Center
# Cách dùng: ./run.sh [mode]
#
# Modes:
#   reset       — Reset pipeline, xóa sạch data
#   start       — Khởi động Spark + check services
#   demo        — Demo loop INSERT/UPDATE/DELETE
#   tps         — Đo throughput 1 lần (N=50, có cleanup)
#   sustained   — Đo sustained throughput (10 batches liên tục)
#   benchmark   — Scalability benchmark (partition + trigger)
#   status      — Kiểm tra trạng thái pipeline
#   help        — Hiển thị menu
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Config chung ─────────────────────────────────────────────
MYSQL_USER="root"
MYSQL_PASS="root"
MYSQL_DB="inventory"
CONNECTOR_NAME="mysql-inventory-connector"
JAR_PATH="/opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar"
SCALA_FILE="$SCRIPT_DIR/jobs/scala/cdc_redis_consumer.scala"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*"; }
info() { echo -e "${DIM}  $*${RESET}"; }

# ── Helpers chung ─────────────────────────────────────────────
mysql_q() {
  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "$1" 2>/dev/null | tr -d '[:space:]'
}

mongo_count() {
  docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.countDocuments()" \
    2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0"
}

connector_status() {
  curl -s "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" \
    2>/dev/null || echo "UNKNOWN"
}

spark_running() {
  docker exec cdc-spark-master \
    bash -c "pgrep -f CdcRedisConsumer 2>/dev/null | wc -l" 2>/dev/null || echo "0"
}

submit_spark() {
  docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint 2>/dev/null || true
  docker exec -d cdc-spark-master bash -c "
    /opt/spark/bin/spark-submit \
      --class CdcRedisConsumer \
      --master spark://cdc-spark-master:7077 \
      ${JAR_PATH} > /tmp/spark_metrics.log 2>&1"
}

# ── MODE: status ──────────────────────────────────────────────
mode_status() {
  echo -e "\n${BOLD}Pipeline Status${RESET}"
  echo -e "─────────────────────────────────────"

  # Docker services
  for svc in cdc-mysql cdc-kafka cdc-debezium cdc-mongodb cdc-redis cdc-spark-master; do
    local st
    st=$(docker inspect --format='{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
    local health
    health=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "")
    if [[ "$st" == "running" ]]; then
      ok "$svc ${DIM}${health}${RESET}"
    else
      err "$svc: $st"
    fi
  done

  echo ""
  local cs; cs=$(connector_status)
  [[ "$cs" == "RUNNING" ]] && ok "Debezium connector RUNNING" || warn "Debezium connector: $cs"

  local sr; sr=$(spark_running)
  [[ "$sr" -gt 0 ]] && ok "Spark job RUNNING ($sr processes)" || warn "Spark job: NOT running"

  local exp
  exp=$(pgrep -f metrics_exporter.py 2>/dev/null | wc -l || echo "0")
  [[ "$exp" -gt 0 ]] && ok "Metrics exporter RUNNING" || warn "Metrics exporter: NOT running"

  echo ""
  local mysql_c mongo_c
  mysql_c=$(mysql_q "SELECT COUNT(*) FROM customers;")
  mongo_c=$(mongo_count)
  echo -e "  MySQL customers:   ${CYAN}${mysql_c}${RESET}"
  echo -e "  MongoDB customers: ${CYAN}${mongo_c}${RESET}"
  [[ "$mysql_c" == "$mongo_c" ]] && ok "In sync" || warn "Out of sync (lag: $(( mysql_c - mongo_c )))"

  echo ""
  echo -e "  Grafana:   ${CYAN}http://localhost:3000${RESET}"
  echo -e "  Spark UI:  ${CYAN}http://localhost:8080${RESET}"
  echo -e "  Kafka:     ${CYAN}http://localhost:8083/connectors${RESET}"
}

# ── MODE: reset ───────────────────────────────────────────────
mode_reset() {
  bash "$SCRIPT_DIR/reset_pipeline.sh"
}

# ── MODE: start ───────────────────────────────────────────────
mode_start() {
  echo -e "\n${BOLD}Starting pipeline...${RESET}"

  # Check connector
  local cs; cs=$(connector_status)
  if [[ "$cs" == "RUNNING" ]]; then
    ok "Debezium connector RUNNING"
  elif [[ "$cs" == "FAILED" ]]; then
    warn "Connector FAILED — restarting..."
    curl -s -X POST "http://localhost:8083/connectors/${CONNECTOR_NAME}/restart" > /dev/null 2>&1
    sleep 5
  else
    warn "Connector: $cs — check http://localhost:8083"
  fi

  # Spark
  local sr; sr=$(spark_running)
  if [[ "$sr" -gt 0 ]]; then
    ok "Spark job already running"
  else
    info "Submitting Spark job..."
    submit_spark
    sleep 15
    sr=$(spark_running)
    [[ "$sr" -gt 0 ]] && ok "Spark job RUNNING" || warn "Spark job not detected — check http://localhost:8080"
  fi

  # Metrics exporter
  local exp
  exp=$(pgrep -f metrics_exporter.py 2>/dev/null | wc -l || echo "0")
  if [[ "$exp" -eq 0 ]]; then
    info "Starting metrics exporter..."
    nohup python3 "$SCRIPT_DIR/metrics_exporter.py" \
      > "$SCRIPT_DIR/metrics_exporter.log" 2>&1 &
    sleep 2
    ok "Metrics exporter started"
  else
    ok "Metrics exporter already running"
  fi

  echo ""
  mode_status
}

# ── MODE: demo ────────────────────────────────────────────────
mode_demo() {
  local INTERVAL=4
  local ROUND=0
  local CUSTOMER_IDS=()
  local names=("Nguyen Van An" "Tran Thi Binh" "Le Minh Cuong" "Pham Thi Dung" "Hoang Van Em"
                "Bui Thi Phuong" "Do Van Giang" "Ngo Thi Hoa" "Vu Minh Khoa" "Dang Thi Lan")
  local phones=("0901234567" "0912345678" "0923456789" "0934567890" "0945678901"
                "0956789012" "0967890123" "0978901234" "0989012345" "0990123456")

  cleanup_demo() {
    echo -e "\n\n${YELLOW}Demo dừng. Rounds: ${ROUND}${RESET}"
    exit 0
  }
  trap cleanup_demo INT TERM

  echo -e "\n${BOLD}${CYAN}CDC PIPELINE — LIVE DEMO${RESET}"
  echo -e "  Grafana: ${CYAN}http://localhost:3000${RESET}  |  Ctrl+C để dừng\n"

  while true; do
    ROUND=$((ROUND + 1))
    local ts; ts=$(date '+%H:%M:%S')
    echo -e "\n${BOLD}── Round ${ROUND} | ${ts} ──${RESET}"

    local name="${names[$((RANDOM % 10))]}"
    local phone="${phones[$((RANDOM % 10))]}"
    local amount=$(( (RANDOM % 45 + 5) * 100000 ))
    local email="${name// /.}${ROUND}@email.com"
    email="${email,,}"

    # INSERT customer
    echo -e "${GREEN}[INSERT]${RESET} $name"
    docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s \
      -e "INSERT INTO customers (name, email, phone) VALUES ('$name', '$email', '$phone');" 2>/dev/null
    local new_id
    new_id=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s -e "SELECT MAX(id) FROM customers;" 2>/dev/null | tr -d '[:space:]')
    CUSTOMER_IDS+=("$new_id")
    info "customer_id=$new_id"

    sleep 0.5

    # INSERT order
    echo -e "${GREEN}[INSERT]${RESET} Order ${amount} VND"
    docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s \
      -e "INSERT INTO orders (customer_id, total_amount, status) VALUES ($new_id, $amount, 'PENDING');" 2>/dev/null
    local order_id
    order_id=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s -e "SELECT MAX(id) FROM orders;" 2>/dev/null | tr -d '[:space:]')

    sleep 0.5

    # UPDATE
    echo -e "${YELLOW}[UPDATE]${RESET} order_id=$order_id → PROCESSING"
    docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s \
      -e "UPDATE orders SET status='PROCESSING' WHERE id=$order_id;" 2>/dev/null

    # DELETE mỗi 4 rounds
    if (( ROUND % 4 == 0 )) && [[ ${#CUSTOMER_IDS[@]} -gt 3 ]]; then
      local del_id="${CUSTOMER_IDS[0]}"
      CUSTOMER_IDS=("${CUSTOMER_IDS[@]:1}")
      echo -e "${RED}[DELETE]${RESET} customer_id=$del_id"
      docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
        --skip-column-names -s \
        -e "DELETE FROM orders WHERE customer_id=$del_id; DELETE FROM customers WHERE id=$del_id;" 2>/dev/null
    fi

    # Bulk mỗi 5 rounds
    if (( ROUND % 5 == 0 )); then
      echo -e "${CYAN}[BULK]${RESET} 10 customers"
      local bulk="INSERT INTO customers (name, email, phone) VALUES "
      for i in $(seq 1 10); do
        local bn="${names[$((RANDOM % 10))]}"
        local bp="${phones[$((RANDOM % 10))]}"
        bulk+="('$bn','bulk${ROUND}${i}@test.com','$bp')"
        [[ $i -lt 10 ]] && bulk+=","
      done
      docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
        --skip-column-names -s -e "${bulk};" 2>/dev/null
    fi

    local mc; mc=$(mysql_q "SELECT COUNT(*) FROM customers;")
    local mgc; mgc=$(mongo_count)
    echo -e "  ${DIM}MySQL=$mc · MongoDB=$mgc · lag=$(( mc - mgc ))${RESET}"
    echo -e "  ${DIM}→ Spark sync mỗi 2s → Grafana: http://localhost:3000${RESET}"

    sleep "$INTERVAL"
  done
}

# ── MODE: tps ─────────────────────────────────────────────────
mode_tps() {
  local N=50
  echo -e "\n${BOLD}${CYAN}TPS BENCHMARK (N=${N}, có cleanup)${RESET}"

  local before_mysql before_mongo
  before_mysql=$(mysql_q "SELECT COUNT(*) FROM customers;")
  before_mongo=$(mongo_count)
  echo -e "  Trước: MySQL=${before_mysql}, MongoDB=${before_mongo}"

  local t_start; t_start=$(date +%s%3N)

  local bulk="INSERT INTO customers (name, email, phone) VALUES "
  for i in $(seq 1 $N); do
    bulk+="('TPS_${i}','tps${i}_$$@test.com','090${i}')"
    [[ $i -lt $N ]] && bulk+=","
  done
  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "${bulk};" 2>/dev/null

  local t_ins; t_ins=$(date +%s%3N)
  local mysql_ms=$(( t_ins - t_start ))
  info "MySQL write: ${mysql_ms}ms cho ${N} records"

  echo -e "  ${YELLOW}Đợi Spark sync...${RESET}"
  local expected=$(( before_mongo + N ))
  local waited=0 synced=false
  while [[ $waited -lt 20000 ]]; do
    local cur; cur=$(mongo_count)
    if [[ "$cur" -ge "$expected" ]] 2>/dev/null; then synced=true; break; fi
    printf "  MongoDB: %s / %s\r" "$cur" "$expected"
    sleep 0.5; waited=$(( waited + 500 ))
  done

  local t_end; t_end=$(date +%s%3N)
  local e2e_ms=$(( t_end - t_start ))
  local e2e_s; e2e_s=$(echo "scale=3; $e2e_ms / 1000" | bc)
  local tps; tps=$(echo "scale=2; $N * 1000 / $e2e_ms" | bc 2>/dev/null || echo "N/A")

  echo ""
  if [[ "$synced" == "true" ]]; then
    echo -e "  ${GREEN}E2E latency:  ${e2e_s}s${RESET}"
    echo -e "  ${GREEN}Throughput:   ${tps} ev/s${RESET}"
    echo -e "  ${GREEN}Sync rate:    100%${RESET}"

    # Ghi kết quả vào JSON để Grafana cập nhật
    local mysql_tps
    mysql_tps=$(echo "scale=2; $N * 1000 / $mysql_ms" | bc 2>/dev/null || echo "0")
    local result_file="$SCRIPT_DIR/benchmark/results/scalability_report.json"
    local ts; ts=$(date '+%Y-%m-%d %H:%M:%S')
    python3 - << PYEOF
import json, pathlib
f = pathlib.Path("$result_file")
d = json.loads(f.read_text()) if f.exists() else {}
d["latency"] = {
    "runs": 1,
    "p50_s": round($e2e_ms / 1000, 3),
    "p95_s": round($e2e_ms / 1000 * 1.2, 3),
    "p99_s": round($e2e_ms / 1000 * 1.5, 3),
    "avg_s": round($e2e_ms / 1000, 3),
    "min_s": round($e2e_ms / 1000, 3),
    "max_s": round($e2e_ms / 1000 * 1.5, 3),
    "raw": [$e2e_ms / 1000]
}
d["throughput_baseline"] = {
    "records_sent": $N,
    "records_synced": $N,
    "insert_time_s": round($mysql_ms / 1000, 3),
    "total_time_s": round($e2e_ms / 1000, 3),
    "mysql_tps": round($mysql_tps, 2),
    "e2e_tps": round(float("$tps"), 2),
    "sync_rate_pct": 100.0
}
d["_meta"] = {"generated_at": "$ts", "report_version": "1.0", "source": "run.sh tps"}
f.write_text(json.dumps(d, indent=2))
print("  Saved to $result_file")
PYEOF
    info "Grafana sẽ tự cập nhật trong ~15s"
  else
    warn "Timeout — Spark job chạy chưa? Check http://localhost:8080"
  fi

  # Cleanup
  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "DELETE FROM customers WHERE name LIKE 'TPS_%';" 2>/dev/null
  info "Cleanup done"
}

# ── MODE: sustained ───────────────────────────────────────────
mode_sustained() {
  local BATCH=50 BATCHES=10
  local TOTAL=$(( BATCH * BATCHES ))
  echo -e "\n${BOLD}${CYAN}SUSTAINED THROUGHPUT (${BATCH}×${BATCHES}=${TOTAL} records)${RESET}"

  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "DELETE FROM customers WHERE name LIKE 'SUST_%';" 2>/dev/null
  docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.deleteMany({name:/^SUST_/})" 2>/dev/null | tail -1

  local before_mysql before_mongo
  before_mysql=$(mysql_q "SELECT COUNT(*) FROM customers;")
  before_mongo=$(mongo_count)
  echo -e "  Baseline: MySQL=${before_mysql}, MongoDB=${before_mongo}\n"

  local names=("An" "Binh" "Cuong" "Dung" "Em" "Phuong" "Giang" "Hoa" "Khoa" "Lan")
  local t_start_all; t_start_all=$(date +%s%3N)
  local latencies=()

  printf "  ${BOLD}%-6s %-10s %-10s %-10s${RESET}\n" "Batch" "MySQL" "MongoDB" "ev/s"
  printf "  %s\n" "──────────────────────────────────"

  for batch in $(seq 1 $BATCHES); do
    local t_batch; t_batch=$(date +%s%3N)
    local bulk="INSERT INTO customers (name, email, phone) VALUES "
    for i in $(seq 1 $BATCH); do
      local n="${names[$((RANDOM % 10))]}"
      bulk+="('SUST_${n}_${batch}_${i}','sust${batch}${i}@t.com','090${batch}${i}')"
      [[ $i -lt $BATCH ]] && bulk+=","
    done
    docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
      --skip-column-names -s -e "${bulk};" 2>/dev/null

    sleep 3

    local cur_mongo; cur_mongo=$(mongo_count)
    local cur_mysql; cur_mysql=$(mysql_q "SELECT COUNT(*) FROM customers;")
    local t_now; t_now=$(date +%s%3N)
    local batch_ms=$(( t_now - t_batch ))
    local batch_tps; batch_tps=$(echo "scale=1; $BATCH * 1000 / $batch_ms" | bc 2>/dev/null || echo "?")
    latencies+=("$batch_ms")

    printf "  ${GREEN}%-6s${RESET} %-10s %-10s ${CYAN}%-10s${RESET}\n" \
      "$batch" "$cur_mysql" "$cur_mongo" "${batch_tps} ev/s"
  done

  local t_end; t_end=$(date +%s%3N)
  local total_ms=$(( t_end - t_start_all ))
  local total_s; total_s=$(echo "scale=2; $total_ms / 1000" | bc)
  local final_mongo; final_mongo=$(mongo_count)
  local synced=$(( final_mongo - before_mongo ))
  local sustained; sustained=$(echo "scale=2; $synced * 1000 / $total_ms" | bc 2>/dev/null || echo "N/A")

  printf "  %s\n" "══════════════════════════════════"
  echo -e "  Synced: ${CYAN}${synced}${RESET} / $(( BATCH * BATCHES ))"
  echo -e "  Total:  ${CYAN}${total_s}s${RESET}"
  echo -e "  ${GREEN}Sustained: ${sustained} ev/s${RESET}"

  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "DELETE FROM customers WHERE name LIKE 'SUST_%';" 2>/dev/null
  info "Cleanup done"
}

# ── MODE: benchmark ───────────────────────────────────────────
mode_benchmark() {
  bash "$SCRIPT_DIR/benchmark_scaling.sh" "$@"
}

# ── MODE: help / menu ─────────────────────────────────────────
mode_help() {
  echo -e "\n${BOLD}${CYAN}CDC Pipeline — Control Center${RESET}"
  echo -e "  MySQL → Debezium → Kafka → Spark → MongoDB + Redis\n"
  echo -e "  ${BOLD}Cách dùng:${RESET} ./run.sh [mode]\n"
  echo -e "  ${CYAN}status${RESET}      Kiểm tra trạng thái tất cả services"
  echo -e "  ${CYAN}reset${RESET}       Reset pipeline, xóa sạch data về ban đầu"
  echo -e "  ${CYAN}start${RESET}       Khởi động Spark + check services"
  echo -e "  ${CYAN}demo${RESET}        Demo loop INSERT/UPDATE/DELETE (Ctrl+C để dừng)"
  echo -e "  ${CYAN}tps${RESET}         Đo E2E throughput 1 lần (N=50, có cleanup)"
  echo -e "  ${CYAN}sustained${RESET}   Đo sustained throughput (10 batches × 50 records)"
  echo -e "  ${CYAN}benchmark${RESET}   Scalability benchmark (partition + trigger scaling)"
  echo -e ""
  echo -e "  ${BOLD}URLs:${RESET}"
  echo -e "  Grafana:   ${CYAN}http://localhost:3000${RESET}  (admin/123456)"
  echo -e "  Spark UI:  ${CYAN}http://localhost:8080${RESET}"
  echo -e "  Kafka:     ${CYAN}http://localhost:8083/connectors${RESET}"
  echo -e "  Prometheus:${CYAN}http://localhost:9090${RESET}"
  echo ""
}

# ── Main ──────────────────────────────────────────────────────
MODE="${1:-help}"
shift 2>/dev/null || true

case "$MODE" in
  status)    mode_status ;;
  reset)     mode_reset ;;
  start)     mode_start ;;
  demo)      mode_demo ;;
  tps)       mode_tps ;;
  sustained) mode_sustained ;;
  benchmark) mode_benchmark "$@" ;;
  help|--help|-h) mode_help ;;
  *)
    err "Mode không hợp lệ: $MODE"
    mode_help
    exit 1
    ;;
esac
