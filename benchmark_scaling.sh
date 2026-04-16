#!/bin/bash
# ============================================================
# benchmark_scaling.sh — CDC Pipeline Scalability Benchmark
#
# Đo thật E2E throughput + latency cho từng config:
#   - Partition: 1, 2, 3, 6
#   - Trigger:   2s, 500ms
#
# Mỗi config: thay đổi partition + rebuild JAR nếu cần,
# restart pipeline, chạy TPS test, ghi kết quả.
#
# Kết quả lưu vào: benchmark/results/scaling_<timestamp>.json
#
# Cách dùng:
#   chmod +x benchmark_scaling.sh
#   ./benchmark_scaling.sh              # chạy tất cả configs
#   ./benchmark_scaling.sh --quick      # chỉ 1,3 partition × 2s trigger
#   ./benchmark_scaling.sh --trigger-only  # chỉ đo 500ms vs 2s (partition=1)
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCALA_FILE="$SCRIPT_DIR/jobs/scala/cdc_redis_consumer.scala"
BUILD_DIR="$SCRIPT_DIR/jobs/scala"
JAR_PATH="/opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar"
JAR_LOCAL="$SCRIPT_DIR/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar"
CONNECTOR_NAME="mysql-inventory-connector"
RESULTS_DIR="$SCRIPT_DIR/benchmark/results"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULT_FILE="$RESULTS_DIR/scaling_${TIMESTAMP}.json"

BENCH_RECORDS=100   # records mỗi TPS test
BENCH_RUNS=5        # số lần lặp mỗi config (lấy median)
WARMUP_RECORDS=20   # warmup trước khi đo

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*"; }
info() { echo -e "${DIM}  $*${RESET}"; }
step() { echo -e "\n${BOLD}${CYAN}══ $* ══${RESET}"; }

# ── Parse args ───────────────────────────────────────────────
MODE="full"
for arg in "$@"; do
  case $arg in
    --quick)        MODE="quick" ;;
    --trigger-only) MODE="trigger" ;;
    --help|-h)
      echo "Usage: $0 [--quick] [--trigger-only]"
      echo "  (no flag)       Tất cả configs: partition 1,2,3,6 × trigger 2s,500ms"
      echo "  --quick         Chỉ partition 1,3 × trigger 2s (nhanh hơn)"
      echo "  --trigger-only  Chỉ đo trigger 2s vs 500ms, partition=1"
      exit 0 ;;
  esac
done

mkdir -p "$RESULTS_DIR"

# ── Configs theo mode ────────────────────────────────────────
case $MODE in
  full)
    PARTITIONS=(1 2 3 6)
    TRIGGERS=("2 seconds" "500 milliseconds")
    ;;
  quick)
    PARTITIONS=(1 3)
    TRIGGERS=("2 seconds")
    ;;
  trigger)
    PARTITIONS=(1)
    TRIGGERS=("2 seconds" "500 milliseconds")
    ;;
esac

TOTAL_CONFIGS=$(( ${#PARTITIONS[@]} * ${#TRIGGERS[@]} ))

# ── Banner ───────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     CDC PIPELINE — SCALABILITY BENCHMARK               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${RESET}"
echo -e "  Mode:        ${BOLD}${MODE}${RESET}"
echo -e "  Partitions:  ${CYAN}${PARTITIONS[*]}${RESET}"
echo -e "  Triggers:    ${CYAN}${TRIGGERS[*]}${RESET}"
echo -e "  Configs:     ${CYAN}${TOTAL_CONFIGS}${RESET} total"
echo -e "  Records/run: ${CYAN}${BENCH_RECORDS}${RESET} × ${BENCH_RUNS} runs"
echo -e "  Results:     ${CYAN}${RESULT_FILE}${RESET}"
echo ""
warn "Script sẽ restart Debezium + Spark mỗi config. Mất ~5-10 phút/config."
echo -e "  Nhấn Enter để tiếp tục, Ctrl+C để hủy..."
read -r

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════

mysql_exec() {
  docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s -e "$1" 2>/dev/null | tr -d '[:space:]'
}

mongo_count() {
  docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.countDocuments()" \
    2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0"
}

wait_kafka_ready() {
  local max=30 n=0
  while [[ $n -lt $max ]]; do
    if docker exec cdc-kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 > /dev/null 2>&1; then
      return 0
    fi
    sleep 2; n=$((n+2))
  done
  warn "Kafka không ready sau ${max}s"
  return 1
}

wait_connector_running() {
  local max=60 n=0
  while [[ $n -lt $max ]]; do
    local state
    state=$(curl -s \
      "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" \
      2>/dev/null || echo "UNKNOWN")
    [[ "$state" == "RUNNING" ]] && return 0
    sleep 3; n=$((n+3))
  done
  warn "Connector không RUNNING sau ${max}s"
  return 1
}

wait_spark_ready() {
  local max=45 n=0
  while [[ $n -lt $max ]]; do
    local running
    running=$(docker exec cdc-spark-master \
      bash -c "pgrep -f CdcRedisConsumer 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    [[ "$running" -gt 0 ]] && return 0
    sleep 3; n=$((n+3))
  done
  warn "Spark job không start sau ${max}s"
  return 1
}

# ── Rebuild JAR với trigger mới ──────────────────────────────
rebuild_jar() {
  local trigger="$1"
  info "Patching trigger → '${trigger}' trong $SCALA_FILE"
  sed -i "s/Trigger.ProcessingTime(\"[^\"]*\")/Trigger.ProcessingTime(\"${trigger}\")/g" \
    "$SCALA_FILE"

  local actual
  actual=$(grep -o "ProcessingTime(\"[^\"]*\")" "$SCALA_FILE" | head -1)
  info "Trigger sau patch: $actual"

  info "Building JAR (sbt package)..."
  cd "$BUILD_DIR"
  if sbt package > /tmp/sbt_build.log 2>&1; then
    ok "JAR built thành công"
  else
    err "sbt build failed. Log:"
    tail -20 /tmp/sbt_build.log
    return 1
  fi
  cd "$SCRIPT_DIR"
}

# ── Set Kafka partition count ────────────────────────────────
set_partitions() {
  local n="$1"
  info "Xóa connector trước khi xóa topic..."
  curl -s -X DELETE \
    "http://localhost:8083/connectors/${CONNECTOR_NAME}" > /dev/null 2>&1 || true
  sleep 8

  info "Xóa topic CDC cũ..."
  for topic in inventory.inventory.customers inventory.inventory.orders; do
    docker exec cdc-kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --delete --topic "$topic" 2>/dev/null || true
  done
  sleep 8

  info "Tạo topics với ${n} partitions..."
  for topic in inventory.inventory.customers inventory.inventory.orders; do
    docker exec cdc-kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --create --topic "$topic" \
      --partitions "$n" \
      --replication-factor 1 2>/dev/null || true
  done
  sleep 3

  local actual_p
  actual_p=$(docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe --topic inventory.inventory.customers 2>/dev/null \
    | grep "PartitionCount:" | awk -F'PartitionCount:' '{print $2}' | awk '{print $1}')
  info "Partition count verified: ${actual_p:-unknown}"
}

# ── Restart Debezium connector ───────────────────────────────
restart_connector() {
  info "Xóa connector cũ..."
  curl -s -X DELETE \
    "http://localhost:8083/connectors/${CONNECTOR_NAME}" > /dev/null 2>&1 || true
  sleep 5

  info "Register connector mới..."
  curl -s -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"${CONNECTOR_NAME}\",
      \"config\": {
        \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
        \"tasks.max\": \"1\",
        \"database.hostname\": \"cdc-mysql\",
        \"database.port\": \"3306\",
        \"database.user\": \"root\",
        \"database.password\": \"root\",
        \"database.server.id\": \"184054\",
        \"topic.prefix\": \"inventory\",
        \"database.include.list\": \"inventory\",
        \"table.include.list\": \"inventory.customers,inventory.orders\",
        \"schema.history.internal.kafka.bootstrap.servers\": \"cdc-kafka:29092\",
        \"schema.history.internal.kafka.topic\": \"schema-changes.inventory\",
        \"snapshot.mode\": \"schema_only\",
        \"include.schema.changes\": \"true\",
        \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
        \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
        \"key.converter.schemas.enable\": \"false\",
        \"value.converter.schemas.enable\": \"false\"
      }
    }" > /dev/null 2>&1

  info "Đợi connector RUNNING..."
  if wait_connector_running; then
    ok "Connector RUNNING"
  else
    warn "Connector chưa RUNNING — tiếp tục anyway"
  fi
}

# ── Restart Spark job ────────────────────────────────────────
restart_spark() {
  info "Kill Spark job cũ..."
  docker exec cdc-spark-master bash -c \
    "pkill -f CdcRedisConsumer 2>/dev/null; exit 0" 2>/dev/null || true
  sleep 3

  info "Xóa checkpoint..."
  docker exec cdc-spark-master rm -rf /tmp/spark-checkpoint 2>/dev/null || true

  info "Submit Spark job (background)..."
  docker exec -d cdc-spark-master bash -c \
    "/opt/spark/bin/spark-submit \
      --class CdcRedisConsumer \
      --master spark://cdc-spark-master:7077 \
      ${JAR_PATH} \
      > /tmp/spark_benchmark.log 2>&1"

  info "Đợi Spark job active..."
  if wait_spark_ready; then
    ok "Spark job RUNNING"
  else
    warn "Spark job chưa active — tiếp tục anyway"
  fi

  info "Warmup 15s..."
  sleep 15
}

# ── Single TPS measurement ───────────────────────────────────
measure_tps() {
  local run_id="$1"

  # Cleanup test records từ run trước
  mysql_exec "DELETE FROM customers WHERE name LIKE 'BENCH_%';" > /dev/null 2>&1 || true
  docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.deleteMany({name: /^BENCH_/})" \
    2>/dev/null > /dev/null || true
  sleep 1

  local before_mongo
  before_mongo=$(mongo_count)

  # Build bulk INSERT
  local bulk_sql="INSERT INTO customers (name, email, phone) VALUES "
  for i in $(seq 1 $BENCH_RECORDS); do
    bulk_sql+="('BENCH_${run_id}_${i}', 'bench${run_id}${i}@test.com', '090${run_id}${i}')"
    [[ $i -lt $BENCH_RECORDS ]] && bulk_sql+=","
  done

  local t_start
  t_start=$(date +%s%3N)

  mysql_exec "$bulk_sql;" > /dev/null 2>&1

  local t_inserted
  t_inserted=$(date +%s%3N)
  local mysql_ms=$(( t_inserted - t_start ))

  # Poll MongoDB — max 30s
  local expected=$(( before_mongo + BENCH_RECORDS ))
  local waited=0 synced=false
  while [[ $waited -lt 30000 ]]; do
    local cur
    cur=$(mongo_count)
    if [[ "$cur" -ge "$expected" ]] 2>/dev/null; then
      synced=true; break
    fi
    sleep 0.5; waited=$(( waited + 500 ))
  done

  local t_end
  t_end=$(date +%s%3N)
  local e2e_ms=$(( t_end - t_start ))

  # Cleanup
  mysql_exec "DELETE FROM customers WHERE name LIKE 'BENCH_%';" > /dev/null 2>&1 || true

  if [[ "$synced" == "true" ]]; then
    echo "${e2e_ms} ${mysql_ms}"
  else
    echo "TIMEOUT 0"
  fi
}

# ════════════════════════════════════════════════════════════
# MAIN BENCHMARK LOOP
# ════════════════════════════════════════════════════════════

RESULTS=()
CURRENT_TRIGGER=""
CURRENT_PARTITIONS=0
CONFIG_NUM=0

for trigger in "${TRIGGERS[@]}"; do
  # Rebuild JAR nếu trigger thay đổi
  if [[ "$trigger" != "$CURRENT_TRIGGER" ]]; then
    step "REBUILD JAR — trigger='${trigger}'"
    rebuild_jar "$trigger"
    CURRENT_TRIGGER="$trigger"
    CURRENT_PARTITIONS=0  # Force restart Spark
  fi

  for partitions in "${PARTITIONS[@]}"; do
    CONFIG_NUM=$(( CONFIG_NUM + 1 ))
    local_tag="p${partitions}_t${trigger// /_}"

    step "CONFIG ${CONFIG_NUM}/${TOTAL_CONFIGS}: partitions=${partitions}, trigger='${trigger}'"

    # Set partitions (nếu thay đổi)
    if [[ "$partitions" != "$CURRENT_PARTITIONS" ]]; then
      set_partitions "$partitions"
      restart_connector
      CURRENT_PARTITIONS="$partitions"
    fi

    # Restart Spark (luôn luôn để đảm bảo JAR mới)
    restart_spark

    # Warmup run (không tính)
    info "Warmup run..."
    bulk_sql="INSERT INTO customers (name, email, phone) VALUES "
    for i in $(seq 1 $WARMUP_RECORDS); do
      bulk_sql+="('WARM_${i}', 'warm${i}@test.com', '090${i}')"
      [[ $i -lt $WARMUP_RECORDS ]] && bulk_sql+=","
    done
    mysql_exec "$bulk_sql;" > /dev/null 2>&1 || true
    sleep 5
    mysql_exec "DELETE FROM customers WHERE name LIKE 'WARM_%';" > /dev/null 2>&1 || true

    # Benchmark runs
    echo -e "\n  ${BOLD}Chạy ${BENCH_RUNS} runs...${RESET}"
    e2e_times=()
    mysql_times=()
    timeouts=0

    printf "  %-6s %-12s %-12s %-12s\n" "Run" "E2E(ms)" "MySQL(ms)" "ev/s"
    printf "  %s\n" "─────────────────────────────────────"

    for run in $(seq 1 $BENCH_RUNS); do
      result=$(measure_tps "$run")
      e2e_ms=$(echo "$result" | awk '{print $1}')
      mysql_ms=$(echo "$result" | awk '{print $2}')

      if [[ "$e2e_ms" == "TIMEOUT" ]]; then
        timeouts=$(( timeouts + 1 ))
        printf "  %-6s %-12s %-12s %-12s\n" "$run" "TIMEOUT" "-" "-"
      else
        e2e_s=$(echo "scale=3; $e2e_ms / 1000" | bc)
        tps=$(echo "scale=1; $BENCH_RECORDS * 1000 / $e2e_ms" | bc 2>/dev/null || echo "?")
        e2e_times+=("$e2e_ms")
        mysql_times+=("$mysql_ms")
        printf "  ${GREEN}%-6s${RESET} %-12s %-12s ${CYAN}%-12s${RESET}\n" \
          "$run" "${e2e_s}s" "${mysql_ms}ms" "${tps} ev/s"
      fi
      sleep 2
    done

    # Tính p50, p95, avg
    if [[ ${#e2e_times[@]} -gt 0 ]]; then
      sorted=$(printf '%s\n' "${e2e_times[@]}" | sort -n)
      n=${#e2e_times[@]}
      p50_idx=$(( n / 2 ))
      p95_idx=$(( n * 95 / 100 ))
      [[ $p95_idx -ge $n ]] && p95_idx=$(( n - 1 ))

      p50_ms=$(echo "$sorted" | sed -n "$(( p50_idx + 1 ))p")
      p95_ms=$(echo "$sorted" | sed -n "$(( p95_idx + 1 ))p")
      sum=0
      for t in "${e2e_times[@]}"; do sum=$(( sum + t )); done
      avg_ms=$(( sum / n ))

      p50_s=$(echo "scale=3; $p50_ms / 1000" | bc)
      p95_s=$(echo "scale=3; $p95_ms / 1000" | bc)
      avg_s=$(echo "scale=3; $avg_ms / 1000" | bc)
      tps_p50=$(echo "scale=2; $BENCH_RECORDS * 1000 / $p50_ms" | bc 2>/dev/null || echo "N/A")

      printf "  %s\n" "─────────────────────────────────────"
      echo -e "  p50=${CYAN}${p50_s}s${RESET}  p95=${CYAN}${p95_s}s${RESET}  avg=${CYAN}${avg_s}s${RESET}  throughput=${CYAN}${tps_p50} ev/s${RESET}"

      # Store result
      RESULTS+=("{\"partitions\":${partitions},\"trigger\":\"${trigger}\",\"p50_s\":${p50_s},\"p95_s\":${p95_s},\"avg_s\":${avg_s},\"tps_p50\":${tps_p50},\"runs\":${n},\"timeouts\":${timeouts}}")
    else
      echo -e "  ${RED}Tất cả runs timeout — config này không đo được${RESET}"
      RESULTS+=("{\"partitions\":${partitions},\"trigger\":\"${trigger}\",\"p50_s\":null,\"p95_s\":null,\"avg_s\":null,\"tps_p50\":null,\"runs\":0,\"timeouts\":${BENCH_RUNS}}")
    fi

  done
done

# ════════════════════════════════════════════════════════════
# LƯU KẾT QUẢ
# ════════════════════════════════════════════════════════════

step "LƯU KẾT QUẢ"

# Build JSON
results_json=$(printf '%s\n' "${RESULTS[@]}" | paste -sd ',' -)
cat > "$RESULT_FILE" << EOF
{
  "meta": {
    "timestamp": "$(date '+%Y-%m-%d %H:%M:%S')",
    "bench_records": ${BENCH_RECORDS},
    "bench_runs": ${BENCH_RUNS},
    "mode": "${MODE}"
  },
  "results": [${results_json}]
}
EOF

ok "Đã lưu: $RESULT_FILE"

# In bảng tổng kết
echo ""
echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  KẾT QUẢ TỔNG HỢP${RESET}"
echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════${RESET}"
printf "\n  %-12s %-14s %-10s %-10s %-12s\n" \
  "Partitions" "Trigger" "p50" "p95" "Throughput"
printf "  %s\n" "──────────────────────────────────────────────────────"

for r in "${RESULTS[@]}"; do
  p=$(echo "$r" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['partitions'])")
  t=$(echo "$r" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['trigger'])")
  p50=$(echo "$r" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(str(d['p50_s'])+'s' if d['p50_s'] else 'TIMEOUT')")
  p95=$(echo "$r" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(str(d['p95_s'])+'s' if d['p95_s'] else 'TIMEOUT')")
  tps=$(echo "$r" | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(str(d['tps_p50'])+' ev/s' if d['tps_p50'] else 'N/A')")
  printf "  %-12s %-14s ${CYAN}%-10s${RESET} %-10s ${GREEN}%-12s${RESET}\n" \
    "$p" "$t" "$p50" "$p95" "$tps"
done

echo ""
echo -e "  Kết quả đầy đủ: ${CYAN}${RESULT_FILE}${RESET}"
echo ""

# Restore trigger về 2 seconds sau khi xong
step "RESTORE — đặt lại trigger=2 seconds"
sed -i 's/Trigger.ProcessingTime("[^"]*")/Trigger.ProcessingTime("2 seconds")/g' "$SCALA_FILE"
cd "$BUILD_DIR" && sbt package > /dev/null 2>&1; cd "$SCRIPT_DIR"
restart_spark > /dev/null 2>&1
ok "Pipeline restored về config chuẩn (trigger=2s)"
echo ""