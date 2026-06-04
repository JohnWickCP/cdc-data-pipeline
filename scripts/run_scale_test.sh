#!/usr/bin/env bash
# run_scale_test.sh — Ma trận horizontal scaling test
#
# Test combinations: Spark workers × Kafka partitions
# Partitions chỉ tăng không giảm → outer loop theo partitions, inner loop theo workers
#
# Usage:
#   bash run_scale_test.sh                                    # quick, 1→3→6 workers × 3→6→12 partitions
#   bash run_scale_test.sh full                               # mode full
#   bash run_scale_test.sh --workers=3,6 --partitions=6,12   # custom matrix
#   bash run_scale_test.sh quick --workers=1,3               # quick mode, only 1 and 3 workers
#
# Kết quả: benchmark/results/history.jsonl → python3 benchmark/compare_runs.py

set -uo pipefail

# ── Parse args ────────────────────────────────────────────────────────────
MODE="quick"
WORKERS_ARG="1,3,6"
PARTITIONS_ARG="3,6,12"

for arg in "$@"; do
    case "$arg" in
        quick|full|stress|realistic) MODE="$arg" ;;
        --workers=*)    WORKERS_ARG="${arg#--workers=}" ;;
        --partitions=*) PARTITIONS_ARG="${arg#--partitions=}" ;;
        --help|-h)
            echo "Usage: bash run_scale_test.sh [mode] [--workers=N,...] [--partitions=N,...]"
            echo ""
            echo "  mode:         quick | full | stress | realistic  (default: quick)"
            echo "  --workers:    danh sách workers cách nhau bởi dấu phẩy  (default: 1,3,6)"
            echo "                Hỗ trợ 1-6. Ví dụ: --workers=3,6"
            echo "  --partitions: danh sách partitions, PHẢI tăng dần       (default: 3,6,12)"
            echo "                Ví dụ: --partitions=6,12"
            echo ""
            echo "Ví dụ:"
            echo "  bash run_scale_test.sh                           # full matrix, quick mode"
            echo "  bash run_scale_test.sh full                      # full matrix, full benchmark"
            echo "  bash run_scale_test.sh --workers=3,6 --partitions=6,12   # 4 runs"
            echo "  bash run_scale_test.sh quick --workers=1 --partitions=3  # 1 run (baseline)"
            exit 0
            ;;
        *) echo "Arg không nhận ra: $arg  (dùng --help)"; exit 1 ;;
    esac
done

# Parse comma-separated arrays
IFS=',' read -ra WORKER_STEPS    <<< "$WORKERS_ARG"
IFS=',' read -ra PARTITION_STEPS <<< "$PARTITIONS_ARG"

# Validate workers: chỉ hỗ trợ 1-6 vì docker-compose chỉ định nghĩa 6 workers
for w in "${WORKER_STEPS[@]}"; do
    if ! [[ "$w" =~ ^[1-6]$ ]]; then
        echo "Lỗi: worker count '$w' không hợp lệ (hỗ trợ 1-6)"
        exit 1
    fi
done

# Đảm bảo partitions tăng dần (Kafka không thể giảm partition)
prev=0
for p in "${PARTITION_STEPS[@]}"; do
    if [ "$p" -le "$prev" ]; then
        echo "Lỗi: --partitions phải tăng dần, nhưng $p <= $prev"
        echo "  Kafka không thể giảm số partition trong một session."
        echo "  Hãy sắp xếp tăng dần, ví dụ: --partitions=3,6,12"
        exit 1
    fi
    prev=$p
done

COMPOSE="docker compose"
TOTAL=$(( ${#PARTITION_STEPS[@]} * ${#WORKER_STEPS[@]} ))
IDX=0

# ── Colors ──────────────────────────────────────────────────────────────
C_BLUE='\033[1;36m'; C_GREEN='\033[0;32m'; C_YELLOW='\033[1;33m'; C_RED='\033[0;31m'; C_X='\033[0m'
log()  { echo -e "\n${C_BLUE}══ $* ══${C_X}"; }
ok()   { echo -e "${C_GREEN}✓ $*${C_X}"; }
warn() { echo -e "${C_YELLOW}⚠ $*${C_X}"; }
err()  { echo -e "${C_RED}✗ $*${C_X}"; }

# ── CPU budget ───────────────────────────────────────────────────────────
# Tự tính SPARK_WORKER_CORES dựa trên số CPU thật để tránh oversubscription.
# Reserve 4 cores cho các service IO-bound (Kafka, MySQL, MongoDB, Redis, Debezium).
# Mục tiêu: workers × cores ≈ SPARK_BUDGET và ≤ 4 cores/worker (sweet spot).
PHYSICAL_CORES=$(nproc 2>/dev/null || echo 16)
SPARK_BUDGET=$(( PHYSICAL_CORES - 4 ))
[ "$SPARK_BUDGET" -lt 2 ] && SPARK_BUDGET=2

cores_for_workers() {
    local n=$1
    local c=$(( SPARK_BUDGET / n ))
    [ "$c" -lt 1 ] && c=1
    [ "$c" -gt 4 ] && c=4
    echo "$c"
}

# ── Worker management ────────────────────────────────────────────────────
set_workers() {
    local n=$1
    local cores; cores=$(cores_for_workers "$n")
    local total=$(( n * cores ))

    log "Set Spark workers = $n × ${cores} cores/worker = ${total} total executor slots"

    # Export để docker compose đọc từ environment khi tạo/recreate container.
    # PHẢI dùng 'up -d --no-deps' (không phải 'start') để container được recreate
    # với giá trị SPARK_WORKER_CORES mới — 'start' chỉ khởi động lại container cũ.
    export SPARK_WORKER_CORES=$cores

    # Luôn dừng workers 4-6 trước (chúng dùng profile scale-workers)
    $COMPOSE --profile scale-workers stop \
        spark-worker-4 spark-worker-5 spark-worker-6 2>/dev/null || true

    # Xác định worker nào cần start / stop trong nhóm 1-3
    local to_start=()
    local to_stop=()
    for i in 1 2 3; do
        if [ "$i" -le "$n" ]; then
            to_start+=("spark-worker-$i")
        else
            to_stop+=("spark-worker-$i")
        fi
    done

    [ "${#to_stop[@]}" -gt 0 ] && $COMPOSE stop "${to_stop[@]}" 2>/dev/null || true

    # up -d --no-deps: recreate container nếu env thay đổi, không recreate dependencies
    $COMPOSE up -d --no-deps "${to_start[@]}"

    # Nếu n > 3, khởi động workers 4 đến n trong profile scale-workers
    if [ "$n" -ge 4 ]; then
        local scale_workers=()
        for i in $(seq 4 "$n"); do
            scale_workers+=("spark-worker-$i")
        done
        $COMPOSE --profile scale-workers up -d --no-deps "${scale_workers[@]}"
    fi

    echo "Đợi Spark re-register workers (30s)..."
    sleep 30

    local alive
    alive=$(curl -sf http://localhost:8080/json/ 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len([w for w in d.get('workers',[]) if w.get('state')=='ALIVE']))" 2>/dev/null || echo "?")
    ok "Spark workers ALIVE: $alive (target: $n, cores/worker: $cores, total slots: $total)"
}

# Đảm bảo Spark app đang chạy — restart nếu cần, đợi cho đến khi có executor thật
ensure_spark_running() {
    local apps
    apps=$(curl -sf http://localhost:8080/json/ 2>/dev/null \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")

    if [ "$apps" -gt 0 ]; then
        ok "Spark app active ($apps)"
        # Đảm bảo checkpoint đã được commit ít nhất 1 batch (đợi thêm 15s)
        sleep 15
        return 0
    fi

    warn "Spark app không chạy — re-submit..."
    # KHÔNG xóa checkpoint: re-submit với checkpoint cũ để tránh replay Kafka backlog.
    # Nếu checkpoint mất (cdc-spark-master bị recreate) → Spark sẽ đọc từ earliest
    # → run đó sẽ bị đánh dấu invalid bởi benchmark (replay scenario).
    $COMPOSE exec -d spark-master /opt/spark/bin/spark-submit \
        --class CdcRedisConsumer \
        --master spark://cdc-spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0 \
        /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar 2>/dev/null

    # Đợi app xuất hiện trong Spark UI (tối đa 90s)
    local waited=0
    while [ "$waited" -lt 90 ]; do
        sleep 5
        waited=$((waited + 5))
        apps=$(curl -sf http://localhost:8080/json/ 2>/dev/null \
            | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('activeapps',[])))" 2>/dev/null || echo "0")
        [ "$apps" -gt 0 ] && break
    done

    if [ "$apps" -gt 0 ]; then
        ok "Spark app re-started ($apps) sau ${waited}s"
        # Đợi thêm 30s để Spark commit checkpoint đầu tiên và stabilize
        echo "Đợi Spark commit batch đầu (30s)..."
        sleep 30
    else
        warn "Spark app vẫn chưa active sau 90s — tiếp tục anyway (run này có thể invalid)"
    fi
}

restore_all() {
    log "Restore về 3 workers mặc định (cleanup)"
    unset SPARK_WORKER_CORES
    $COMPOSE up -d --no-deps spark-worker-1 spark-worker-2 spark-worker-3 2>/dev/null || true
    $COMPOSE --profile scale-workers stop \
        spark-worker-4 spark-worker-5 spark-worker-6 2>/dev/null || true
}

trap restore_all EXIT

# ── Sanity check ─────────────────────────────────────────────────────────
log "Kiểm tra điều kiện trước khi chạy"

if ! curl -sf http://localhost:8000/metrics > /dev/null 2>&1; then
    err "Metrics exporter không chạy (port 8000). Hãy chạy bash start.sh trước."
    exit 1
fi

if ! curl -sf http://localhost:8080/json/ > /dev/null 2>&1; then
    err "Spark Master không chạy (port 8080). Hãy chạy bash start.sh trước."
    exit 1
fi

ok "Stack đang chạy, bắt đầu scale test"

# ── Hardware summary ──────────────────────────────────────────────────────
echo ""
echo "  Hardware : $(nproc) cores, $(free -m | awk '/^Mem:/{printf "%.0fGB", $2/1024}') RAM"
echo "  SPARK_BUDGET: $(( PHYSICAL_CORES - 4 )) cores ($(nproc) physical − 4 reserved)"
echo ""
echo "  Cấu hình cores/worker:"
for w in "${WORKER_STEPS[@]}"; do
    c=$(cores_for_workers "$w")
    printf "    %d workers × %d cores = %d executor slots\n" "$w" "$c" "$(( w * c ))"
done

# ── Print kế hoạch ───────────────────────────────────────────────────────
echo ""
echo "  Mode:        $MODE"
echo "  Workers:     ${WORKER_STEPS[*]}"
echo "  Partitions:  ${PARTITION_STEPS[*]}"
echo "  Tổng runs:   $TOTAL"
echo ""

# ── Ma trận test ─────────────────────────────────────────────────────────
# Outer loop: partitions (tăng dần — không bao giờ giảm)
# Inner loop: workers    (stop/start tự do)

START_TIME=$(date +%s)

for partitions in "${PARTITION_STEPS[@]}"; do
    for workers in "${WORKER_STEPS[@]}"; do
        IDX=$((IDX + 1))
        ELAPSED=$(( $(date +%s) - START_TIME ))
        log "[$IDX/$TOTAL] workers=$workers × partitions=$partitions  (đã chạy ${ELAPSED}s)"

        set_workers "$workers"
        ensure_spark_running

        if ! python3 benchmark/run_benchmark_v4.py "$MODE" --partitions "$partitions"; then
            warn "Benchmark thất bại tại workers=$workers × partitions=$partitions, tiếp tục..."
        else
            ok "Xong combination [$IDX/$TOTAL]"
        fi

        echo "Nghỉ 10s trước combination tiếp theo..."
        sleep 10
    done
done

# ── Kết quả ─────────────────────────────────────────────────────────────
TOTAL_TIME=$(( $(date +%s) - START_TIME ))
log "HOÀN THÀNH — Tổng thời gian: ${TOTAL_TIME}s"

echo ""
echo "  Xem kết quả so sánh:"
echo "    python3 benchmark/compare_runs.py"
echo ""
echo "  Kết quả thô:"
echo "    benchmark/results/history.jsonl"
echo ""
