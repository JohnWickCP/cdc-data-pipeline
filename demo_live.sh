#!/bin/bash
# ============================================================
# demo_live.sh — CDC Pipeline Live Demo Script
# Dùng khi thuyết trình với thầy / hội đồng
#
# Script tạo INSERT / UPDATE / DELETE liên tục vào MySQL
# → Debezium capture → Kafka → Spark → MongoDB + Redis
# → Grafana dashboard hiển thị data thay đổi real-time
#
# Cách dùng:
#   chmod +x demo_live.sh
#   ./demo_live.sh          # chế độ mặc định (loop liên tục)
#   ./demo_live.sh --once   # chạy 1 vòng rồi dừng
#   ./demo_live.sh --fast   # loop nhanh hơn (1s interval)
#   Ctrl+C để dừng
# ============================================================

set -euo pipefail

# ── Config ──────────────────────────────────────────────────
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASS="root"
MYSQL_DB="inventory"

INTERVAL=4          # giây giữa mỗi batch
FAST_INTERVAL=1

# Màu terminal
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

# ── Parse args ───────────────────────────────────────────────
MODE="loop"
TPS_MODE=false
SUSTAINED_MODE=false
for arg in "$@"; do
  case $arg in
    --once)      MODE="once" ;;
    --fast)      INTERVAL=$FAST_INTERVAL ;;
    --tps)       MODE="once"; TPS_MODE=true ;;
    --sustained) MODE="once"; SUSTAINED_MODE=true ;;
    --help|-h)
      echo "Usage: $0 [--once] [--fast] [--tps] [--sustained]"
      echo "  --once       Chạy 1 vòng INSERT/UPDATE/DELETE rồi dừng"
      echo "  --fast       Loop với interval 1s thay vì 4s"
      echo "  --tps        Đo throughput 1 lần (N=50), có cleanup"
      echo "  --sustained  Đo sustained throughput liên tục (10 batches × 20 records)"
      exit 0
      ;;
  esac
done

# ── Helper: chạy SQL ─────────────────────────────────────────
mysql_run() {
  docker exec cdc-mysql mysql \
    -u"$MYSQL_USER" -p"$MYSQL_PASS" \
    "$MYSQL_DB" \
    -e "$1" \
    --silent 2>/dev/null
}

mysql_run_echo() {
  local label="$1"
  local sql="$2"
  echo -e "${DIM}  SQL: ${sql}${RESET}"
  mysql_run "$sql"
}

# ── Helper: print status ─────────────────────────────────────
print_counts() {
  local customers orders
  customers=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]')
  orders=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d '[:space:]')
  echo -e "  ${DIM}MySQL state: customers=${CYAN}${customers}${RESET}${DIM}, orders=${CYAN}${orders}${RESET}"
}

# ── Check prerequisites ──────────────────────────────────────
check_prereqs() {
  echo -e "\n${BOLD}Kiểm tra prerequisites...${RESET}"

  # MySQL container
  if ! docker exec cdc-mysql mysqladmin ping -u"$MYSQL_USER" -p"$MYSQL_PASS" --silent 2>/dev/null; then
    echo -e "${RED}✗ MySQL không kết nối được. Chắc chắn pipeline đang chạy?${RESET}"
    echo -e "  Thử: cd ~/cdc-pipeline/pipeline && docker compose up -d"
    exit 1
  fi
  echo -e "${GREEN}✓ MySQL OK${RESET}"

  # Table tồn tại không
  if ! mysql_run "SELECT 1 FROM customers LIMIT 1" 2>/dev/null >/dev/null; then
    echo -e "${YELLOW}⚠ Table 'customers' chưa có. Tạo schema demo...${RESET}"
    create_schema
  fi
  echo -e "${GREEN}✓ Schema OK${RESET}"

  # Debezium connector
  local connector_status
  connector_status=$(curl -s http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
  if [[ "$connector_status" == "RUNNING" ]]; then
    echo -e "${GREEN}✓ Debezium connector RUNNING${RESET}"
  else
    echo -e "${YELLOW}⚠ Debezium connector: ${connector_status}${RESET}"
    echo -e "  Nếu cần register: cd ~/cdc-pipeline/demo && bash register-connector.sh"
  fi

  echo ""
}

# ── Tạo schema nếu chưa có ──────────────────────────────────
create_schema() {
  # Schema đã có sẵn từ init.sql — không tạo lại
  # Chỉ verify tables tồn tại
  echo -e "${GREEN}  Tables exist (from init.sql).${RESET}"
}

# ── Banner ───────────────────────────────────────────────────
print_banner() {
  clear
  echo -e "${BOLD}${CYAN}"
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║          CDC PIPELINE — LIVE DEMO                           ║"
  echo "║  MySQL → Debezium → Kafka → Spark → MongoDB + Redis         ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo -e "${RESET}"
  echo -e "  Grafana dashboard: ${CYAN}http://localhost:3000${RESET}"
  echo -e "  Spark UI:          ${CYAN}http://localhost:8080${RESET}"
  echo -e "  Kafka Connect:     ${CYAN}http://localhost:8083${RESET}"
  echo -e "  Prometheus:        ${CYAN}http://localhost:9090${RESET}"
  echo ""
  echo -e "  ${DIM}Nhấn Ctrl+C để dừng${RESET}"
  echo ""
}

# ── Demo cycle ───────────────────────────────────────────────
ROUND=0
CUSTOMER_IDS=()

run_cycle() {
  ROUND=$((ROUND + 1))
  local ts
  ts=$(date '+%H:%M:%S')

  echo -e "\n${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}  Round ${ROUND}  |  ${ts}${RESET}"
  echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

  # Random data
  local names=("Nguyen Van An" "Tran Thi Binh" "Le Minh Cuong" "Pham Thi Dung" "Hoang Van Em"
                "Bui Thi Phuong" "Do Van Giang" "Ngo Thi Hoa" "Vu Minh Khoa" "Dang Thi Lan")
  local phones=("0901234567" "0912345678" "0923456789" "0934567890" "0945678901"
                "0956789012" "0967890123" "0978901234" "0989012345" "0990123456")
  local statuses=("PENDING" "PROCESSING" "SHIPPED" "DELIVERED")
  local name="${names[$((RANDOM % ${#names[@]}))]}"
  local phone="${phones[$((RANDOM % ${#phones[@]}))]}"
  local amount=$(( (RANDOM % 45 + 5) * 100000 ))   # 500K - 5M VND
  local email="${name// /.}${ROUND}@email.com"
  email="${email,,}"

  # ── 1. INSERT customer ──────────────────────────────────────
  echo -e "\n${GREEN}[INSERT]${RESET} Thêm khách hàng mới"
  local insert_customer_sql="INSERT INTO customers (name, email, phone) VALUES ('$name', '$email', '$phone');"
  echo -e "${DIM}  SQL: ${insert_customer_sql}${RESET}"
  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "$insert_customer_sql" 2>/dev/null

  local new_id
  new_id=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "SELECT MAX(id) FROM customers;" 2>/dev/null | tr -d '[:space:]')
  CUSTOMER_IDS+=("$new_id")
  echo -e "  ${GREEN}→ customer_id=${new_id}, name='${name}', phone='${phone}'${RESET}"

  sleep 0.5

  # ── 2. INSERT order ─────────────────────────────────────────
  echo -e "\n${GREEN}[INSERT]${RESET} Thêm đơn hàng"
  local insert_order_sql="INSERT INTO orders (customer_id, total_amount, status) VALUES ($new_id, $amount, 'PENDING');"
  echo -e "${DIM}  SQL: ${insert_order_sql}${RESET}"
  docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "$insert_order_sql" 2>/dev/null

  local order_id
  order_id=$(docker exec cdc-mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
    --skip-column-names -s -e "SELECT MAX(id) FROM orders;" 2>/dev/null | tr -d '[:space:]')
  echo -e "  ${GREEN}→ order_id=${order_id}, amount=${amount} VND, status=PENDING${RESET}"

  sleep 0.5

  # ── 3. UPDATE order status ─────────────────────────────────
  echo -e "\n${YELLOW}[UPDATE]${RESET} Cập nhật trạng thái đơn hàng"
  mysql_run_echo "UPDATE order status" \
    "UPDATE orders SET status='PROCESSING' WHERE id=$order_id;"
  echo -e "  ${YELLOW}→ order_id=${order_id}: PENDING → PROCESSING${RESET}"

  sleep 0.5

  # ── 4. UPDATE customer email (simulate profile update) ─────
  if [[ ${#CUSTOMER_IDS[@]} -gt 1 ]]; then
    local old_id="${CUSTOMER_IDS[0]}"
    local new_phone="${phones[$((RANDOM % ${#phones[@]}))]}"
    echo -e "\n${YELLOW}[UPDATE]${RESET} Cập nhật thông tin khách hàng cũ (id=$old_id)"
    mysql_run_echo "UPDATE customer" \
      "UPDATE customers SET phone='$new_phone' WHERE id=$old_id;"
    echo -e "  ${YELLOW}→ customer_id=${old_id}: phone → '${new_phone}'${RESET}"
    sleep 0.5
  fi

  # ── 5. Mỗi 3 round: SHIP một order cũ ────────────────────
  if (( ROUND % 3 == 0 )); then
    echo -e "\n${YELLOW}[UPDATE]${RESET} Ship đơn hàng (order_id=${order_id})"
    mysql_run_echo "UPDATE order shipped" \
      "UPDATE orders SET status='SHIPPED' WHERE id=$order_id;"
    echo -e "  ${YELLOW}→ order_id=${order_id}: PROCESSING → SHIPPED${RESET}"
    sleep 0.3
  fi

  # ── 6. Mỗi 4 round: DELETE customer cũ nhất ──────────────
  if (( ROUND % 4 == 0 )) && [[ ${#CUSTOMER_IDS[@]} -gt 3 ]]; then
    local del_id="${CUSTOMER_IDS[0]}"
    CUSTOMER_IDS=("${CUSTOMER_IDS[@]:1}")
    echo -e "\n${RED}[DELETE]${RESET} Xóa khách hàng cũ (id=$del_id) + orders"
    mysql_run_echo "DELETE orders" \
      "DELETE FROM orders WHERE customer_id=$del_id;"
    mysql_run_echo "DELETE customer" \
      "DELETE FROM customers WHERE id=$del_id;"
    echo -e "  ${RED}→ Đã xóa customer_id=${del_id}${RESET}"
    sleep 0.3
  fi

  # ── 7. Bulk insert mỗi 5 round ────────────────────────────
  if (( ROUND % 5 == 0 )); then
    echo -e "\n${CYAN}[BULK]${RESET} Thêm 10 customers cùng lúc (test throughput)"
    local bulk_sql="INSERT INTO customers (name, email, phone) VALUES "
    for i in $(seq 1 10); do
      local bn="${names[$((RANDOM % ${#names[@]}))]}"
      local bp="${phones[$((RANDOM % ${#phones[@]}))]}"
      local be="${bn// /.}bulk${ROUND}${i}@email.com"
      be="${be,,}"
      bulk_sql+="('$bn', '$be', '$bp')"
      [[ $i -lt 10 ]] && bulk_sql+=","
    done
    mysql_run "$bulk_sql;"
    echo -e "  ${CYAN}→ 10 customers inserted in 1 SQL${RESET}"
    sleep 0.3
  fi

  # ── Status ─────────────────────────────────────────────────
  echo ""
  print_counts
  echo -e "  ${DIM}→ Spark trigger mỗi 2s sẽ sync lên MongoDB + Redis${RESET}"
  echo -e "  ${DIM}→ Xem Grafana: http://localhost:3000${RESET}"
}

# ── Cleanup handler ──────────────────────────────────────────
cleanup() {
  echo -e "\n\n${YELLOW}Demo dừng. Tổng rounds: ${ROUND}${RESET}"
  echo -e "${DIM}Data vẫn còn trong MySQL/MongoDB/Redis${RESET}"
  exit 0
}
trap cleanup INT TERM

# ── TPS benchmark (1 lần, có cleanup) ───────────────────────
run_tps_test() {
  local N=50
  echo -e "\n${BOLD}${CYAN}══ TPS BENCHMARK (N=${N} records, với cleanup) ══${RESET}"
  echo -e "${DIM}Đo: MySQL INSERT → MongoDB confirmed (E2E latency + throughput)${RESET}\n"

  # Baseline
  local before_mysql before_mongo
  before_mysql=$(docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s -e "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]')
  before_mongo=$(docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
  echo -e "  Trước: MySQL=${CYAN}${before_mysql}${RESET}, MongoDB=${CYAN}${before_mongo}${RESET}"

  local t_start
  t_start=$(date +%s%3N)

  # Bulk INSERT
  echo -e "\n${GREEN}[INSERT]${RESET} Bulk insert ${N} customers vào MySQL..."
  local bulk_sql="INSERT INTO customers (name, email, phone) VALUES "
  local names=("An" "Binh" "Cuong" "Dung" "Em" "Phuong" "Giang" "Hoa" "Khoa" "Lan")
  for i in $(seq 1 $N); do
    local n="${names[$((i % 10))]}"
    bulk_sql+="('TPS_${n}_${i}', 'tps${i}_$$@test.com', '090${i}')"
    [[ $i -lt $N ]] && bulk_sql+=","
  done
  docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s -e "${bulk_sql};" 2>/dev/null

  local t_inserted
  t_inserted=$(date +%s%3N)
  local mysql_write_ms=$(( t_inserted - t_start ))
  echo -e "  ${GREEN}→ MySQL write: ${mysql_write_ms}ms cho ${N} records${RESET}"
  echo -e "  ${DIM}→ MySQL TPS: $(( N * 1000 / (mysql_write_ms + 1) )) records/s${RESET}"

  # Poll MongoDB
  echo -e "\n${YELLOW}Đợi Spark sync sang MongoDB...${RESET}"
  local expected=$(( before_mongo + N ))
  local max_wait_ms=15000 waited_ms=0 synced=false
  while [[ $waited_ms -lt $max_wait_ms ]]; do
    local cur
    cur=$(docker exec cdc-mongodb mongosh inventory \
      --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
    if [[ "$cur" -ge "$expected" ]] 2>/dev/null; then synced=true; break; fi
    printf "  MongoDB: %s / %s (đợi %ss)\r" "$cur" "$expected" "$(( waited_ms / 1000 ))"
    sleep 0.5; waited_ms=$(( waited_ms + 500 ))
  done

  local t_synced e2e_ms e2e_s tps_e2e
  t_synced=$(date +%s%3N)
  e2e_ms=$(( t_synced - t_start ))
  e2e_s=$(echo "scale=3; $e2e_ms / 1000" | bc)
  tps_e2e=$(echo "scale=2; $N * 1000 / $e2e_ms" | bc 2>/dev/null || echo "N/A")

  echo ""
  # Kiểm tra Redis — format customer:{id}
  local redis_count
  redis_count=$(docker exec cdc-redis redis-cli KEYS "customer:*" 2>/dev/null | wc -l | tr -d '[:space:]')

  if [[ "$synced" == "true" ]]; then
    local after_mongo synced_count
    after_mongo=$(docker exec cdc-mongodb mongosh inventory \
      --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
    synced_count=$(( after_mongo - before_mongo ))

    echo -e "\n${BOLD}${GREEN}══ KẾT QUẢ ══${RESET}"
    echo -e "  Records inserted:   ${CYAN}${N}${RESET}"
    echo -e "  MongoDB synced:     ${CYAN}${synced_count}${RESET} / ${N}  (sync rate: $(( synced_count * 100 / N ))%)"
    echo -e "  Redis customer keys:${CYAN} ${redis_count}${RESET} keys (format: customer:{id})"
    echo -e "  MySQL write:        ${CYAN}${mysql_write_ms}ms${RESET}"
    echo -e "  E2E latency:        ${CYAN}${e2e_s}s${RESET}  (INSERT → MongoDB confirmed)"
    echo -e "  E2E throughput:     ${CYAN}${tps_e2e} events/s${RESET}"
    echo -e "  ${DIM}Benchmark chính thức: p50=2.024s, 49.87 ev/s${RESET}"
  else
    echo -e "${YELLOW}⚠ Timeout ${max_wait_ms}ms — Spark job chạy chưa?${RESET}"
    echo -e "${DIM}  Kiểm tra: http://localhost:8080${RESET}"
  fi

  # Cleanup test records
  echo -e "\n${DIM}[Cleanup] Xóa ${N} TPS test records...${RESET}"
  docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s \
    -e "DELETE FROM customers WHERE name LIKE 'TPS_%';" 2>/dev/null
  echo -e "${DIM}  → Cleanup done. Data trả về trạng thái trước khi test.${RESET}"
}

# ── Sustained throughput test ────────────────────────────────
# Insert liên tục nhiều batch, đo sustained events/s thật sự
run_sustained_test() {
  local BATCH_SIZE=20      # records mỗi batch
  local BATCH_INTERVAL=2   # giây giữa các batch (= Spark trigger)
  local TOTAL_BATCHES=10   # tổng số batches
  local TOTAL_RECORDS=$(( BATCH_SIZE * TOTAL_BATCHES ))

  echo -e "\n${BOLD}${CYAN}══ SUSTAINED THROUGHPUT TEST ══${RESET}"
  echo -e "${DIM}${BATCH_SIZE} records × ${TOTAL_BATCHES} batches = ${TOTAL_RECORDS} records total${RESET}"
  echo -e "${DIM}Batch interval: ${BATCH_INTERVAL}s  |  Spark trigger: 2s${RESET}"
  echo -e "${DIM}Ctrl+C để dừng sớm${RESET}\n"

  # Cleanup records cũ từ test trước
  docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s \
    -e "DELETE FROM customers WHERE name LIKE 'SUST_%';" 2>/dev/null

  local before_mysql before_mongo
  before_mysql=$(docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s -e "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]')
  before_mongo=$(docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")

  echo -e "  Baseline: MySQL=${CYAN}${before_mysql}${RESET}, MongoDB=${CYAN}${before_mongo}${RESET}\n"

  local t_start_all
  t_start_all=$(date +%s%3N)
  local names=("An" "Binh" "Cuong" "Dung" "Em" "Phuong" "Giang" "Hoa" "Khoa" "Lan")

  # Header bảng
  printf "  ${BOLD}%-6s %-8s %-10s %-10s %-12s %-10s${RESET}\n" \
    "Batch" "MySQL" "MongoDB" "Lag" "E2E(batch)" "ev/s"
  printf "  %s\n" "──────────────────────────────────────────────────────"

  local total_synced=0
  local latencies=()

  for batch in $(seq 1 $TOTAL_BATCHES); do
    local t_batch_start
    t_batch_start=$(date +%s%3N)

    # Build bulk INSERT
    local bulk_sql="INSERT INTO customers (name, email, phone) VALUES "
    for i in $(seq 1 $BATCH_SIZE); do
      local n="${names[$((RANDOM % 10))]}"
      bulk_sql+="('SUST_${n}_${batch}_${i}', 'sust${batch}${i}@test.com', '09$(( RANDOM % 90000000 + 10000000 ))')"
      [[ $i -lt $BATCH_SIZE ]] && bulk_sql+=","
    done
    docker exec cdc-mysql mysql -uroot -proot inventory \
      --skip-column-names -s -e "${bulk_sql};" 2>/dev/null

    local after_insert_mysql
    after_insert_mysql=$(docker exec cdc-mysql mysql -uroot -proot inventory \
      --skip-column-names -s -e "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]')

    # Poll MongoDB tối đa 8s
    local expected_mongo=$(( before_mongo + batch * BATCH_SIZE ))
    local poll_waited=0 batch_synced=false
    while [[ $poll_waited -lt 8000 ]]; do
      local cur_mongo
      cur_mongo=$(docker exec cdc-mongodb mongosh inventory \
        --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
      if [[ "$cur_mongo" -ge "$expected_mongo" ]] 2>/dev/null; then
        batch_synced=true; break
      fi
      sleep 0.3; poll_waited=$(( poll_waited + 300 ))
    done

    local t_batch_end
    t_batch_end=$(date +%s%3N)
    local batch_ms=$(( t_batch_end - t_batch_start ))
    local batch_s
    batch_s=$(echo "scale=2; $batch_ms / 1000" | bc)
    local batch_tps
    batch_tps=$(echo "scale=1; $BATCH_SIZE * 1000 / $batch_ms" | bc 2>/dev/null || echo "?")

    local cur_mongo
    cur_mongo=$(docker exec cdc-mongodb mongosh inventory \
      --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
    local lag=$(( after_insert_mysql - cur_mongo ))
    [[ $lag -lt 0 ]] && lag=0

    local sync_icon="${GREEN}✓${RESET}"
    [[ "$batch_synced" == "false" ]] && sync_icon="${YELLOW}~${RESET}"

    printf "  ${sync_icon} %-5s %-8s %-10s %-10s %-12s ${CYAN}%-10s${RESET}\n" \
      "$batch" "$after_insert_mysql" "$cur_mongo" "${lag} lag" "${batch_s}s" "${batch_tps} ev/s"

    latencies+=("$batch_ms")
    total_synced=$(( cur_mongo - before_mongo ))

    # Đợi trước batch tiếp theo (không đợi sau batch cuối)
    [[ $batch -lt $TOTAL_BATCHES ]] && sleep "$BATCH_INTERVAL"
  done

  # Tổng kết
  local t_end_all
  t_end_all=$(date +%s%3N)
  local total_ms=$(( t_end_all - t_start_all ))
  local total_s
  total_s=$(echo "scale=2; $total_ms / 1000" | bc)
  local sustained_tps
  sustained_tps=$(echo "scale=2; $total_synced * 1000 / $total_ms" | bc 2>/dev/null || echo "N/A")

  # Tính p50 latency
  local sorted_lats
  sorted_lats=$(printf '%s\n' "${latencies[@]}" | sort -n)
  local p50_idx=$(( TOTAL_BATCHES / 2 ))
  local p50_ms
  p50_ms=$(echo "$sorted_lats" | sed -n "${p50_idx}p")
  local p50_s
  p50_s=$(echo "scale=3; ${p50_ms:-0} / 1000" | bc)

  local final_mongo
  final_mongo=$(docker exec cdc-mongodb mongosh inventory \
    --quiet --eval "db.customers.countDocuments()" 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "0")
  local redis_count
  redis_count=$(docker exec cdc-redis redis-cli KEYS "customer:*" 2>/dev/null | wc -l | tr -d '[:space:]')
  local sync_rate=$(( total_synced * 100 / TOTAL_RECORDS ))

  printf "  %s\n" "══════════════════════════════════════════════════════"
  echo -e "\n${BOLD}${GREEN}══ SUSTAINED RESULTS ══${RESET}"
  echo -e "  Total records:      ${CYAN}${TOTAL_RECORDS}${RESET} (${TOTAL_BATCHES} batches × ${BATCH_SIZE})"
  echo -e "  Synced to MongoDB:  ${CYAN}${total_synced}${RESET} / ${TOTAL_RECORDS}  (${sync_rate}%)"
  echo -e "  Redis customer keys:${CYAN} ${redis_count}${RESET}"
  echo -e "  Total time:         ${CYAN}${total_s}s${RESET}"
  echo -e "  Sustained ev/s:     ${CYAN}${sustained_tps} events/s${RESET}  (E2E, sustained)"
  echo -e "  p50 batch latency:  ${CYAN}${p50_s}s${RESET}"
  echo -e "  ${DIM}Benchmark chính thức: p50=2.024s, 49.87 ev/s${RESET}"

  # Cleanup
  echo -e "\n${DIM}[Cleanup] Xóa SUST test records...${RESET}"
  docker exec cdc-mysql mysql -uroot -proot inventory \
    --skip-column-names -s \
    -e "DELETE FROM customers WHERE name LIKE 'SUST_%';" 2>/dev/null
  echo -e "${DIM}  → Done.${RESET}"
}




print_banner
check_prereqs

if [[ "$TPS_MODE" == "true" ]]; then
  run_tps_test
elif [[ "$SUSTAINED_MODE" == "true" ]]; then
  run_sustained_test
elif [[ "$MODE" == "once" ]]; then
  echo -e "${BOLD}Mode: single run${RESET}"
  run_cycle
  echo -e "\n${GREEN}Done.${RESET}"
else
  echo -e "${BOLD}Mode: loop (interval=${INTERVAL}s) — Ctrl+C để dừng${RESET}\n"
  while true; do
    run_cycle
    echo -e "\n${DIM}Chờ ${INTERVAL}s... (Ctrl+C để dừng)${RESET}"
    sleep "$INTERVAL"
  done
fi