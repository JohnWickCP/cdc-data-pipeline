#!/usr/bin/env bash
# run_scale_test.sh — Ma trận horizontal scaling test
#
# Test combinations: Spark workers (1→3→6) × Kafka partitions (3→6→12)
# Partitions chỉ tăng không giảm → outer loop theo partitions, inner loop theo workers
#
# Usage:
#   bash run_scale_test.sh          # mode quick (~3 phút/combo, tổng ~30 phút)
#   bash run_scale_test.sh full     # mode full  (~10 phút/combo, tổng ~90 phút)
#
# Kết quả: benchmark/results/history.jsonl → python3 benchmark/compare_runs.py

set -uo pipefail

MODE=${1:-quick}
COMPOSE="docker compose"

# Ma trận test — partitions PHẢI tăng dần (Kafka không giảm được partition)
PARTITION_STEPS=(3 6 12)
WORKER_STEPS=(1 3 6)

TOTAL=$(( ${#PARTITION_STEPS[@]} * ${#WORKER_STEPS[@]} ))
IDX=0

# ── Colors ──────────────────────────────────────────────────────────────
C_BLUE='\033[1;36m'; C_GREEN='\033[0;32m'; C_YELLOW='\033[1;33m'; C_RED='\033[0;31m'; C_X='\033[0m'
log()  { echo -e "\n${C_BLUE}══ $* ══${C_X}"; }
ok()   { echo -e "${C_GREEN}✓ $*${C_X}"; }
warn() { echo -e "${C_YELLOW}⚠ $*${C_X}"; }
err()  { echo -e "${C_RED}✗ $*${C_X}"; }

# ── Worker management ────────────────────────────────────────────────────
set_workers() {
    local n=$1
    log "Set Spark workers = $n"

    case $n in
        1)
            $COMPOSE stop spark-worker-2 spark-worker-3 2>/dev/null || true
            $COMPOSE --profile scale-workers stop \
                spark-worker-4 spark-worker-5 spark-worker-6 2>/dev/null || true
            ;;
        3)
            $COMPOSE start spark-worker-2 spark-worker-3
            $COMPOSE --profile scale-workers stop \
                spark-worker-4 spark-worker-5 spark-worker-6 2>/dev/null || true
            ;;
        6)
            $COMPOSE start spark-worker-2 spark-worker-3
            $COMPOSE --profile scale-workers up -d \
                spark-worker-4 spark-worker-5 spark-worker-6
            ;;
        *)
            err "Worker count không hợp lệ: $n (chỉ hỗ trợ 1, 3, 6)"
            exit 1
            ;;
    esac

    echo "Đợi Spark register workers (25s)..."
    sleep 25

    # Verify worker count qua Spark Master API
    local alive
    alive=$(curl -sf http://localhost:8080/json/ 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(len([w for w in d.get('workers',[]) if w.get('state')=='ALIVE']))" 2>/dev/null || echo "?")
    ok "Spark workers ALIVE: $alive (target: $n)"
}

restore_all() {
    log "Restore toàn bộ workers (cleanup)"
    $COMPOSE start spark-worker-1 spark-worker-2 spark-worker-3 2>/dev/null || true
    $COMPOSE --profile scale-workers up -d \
        spark-worker-4 spark-worker-5 spark-worker-6 2>/dev/null || true
}

# Restore khi script kết thúc (kể cả lỗi)
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

# ── Print kế hoạch ───────────────────────────────────────────────────────
echo ""
echo "  Mode:       $MODE"
echo "  Workers:    ${WORKER_STEPS[*]}"
echo "  Partitions: ${PARTITION_STEPS[*]}"
echo "  Tổng runs:  $TOTAL"
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
