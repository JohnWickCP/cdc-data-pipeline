#!/usr/bin/env bash
# vm_run.sh — Khởi động toàn bộ pipeline + demo + recorder trong một lệnh
#
# Usage:
#   bash vm_run.sh                    # start pipeline + demo + recorder
#   bash vm_run.sh --bench            # + chạy benchmark full sau khi ready
#   bash vm_run.sh --bench=quick      # + benchmark quick mode
#   bash vm_run.sh --profile=server   # đổi profile (default: vm)
#   bash vm_run.sh --no-recorder      # không tự động record
#
# Ctrl+C → dừng demo server + recorder, in summary

set -euo pipefail
export MSYS_NO_PATHCONV=1

# ── Parse args ────────────────────────────────────────────────────────────
PROFILE="vm"
BENCH_MODE=""
RUN_RECORDER=true

for arg in "$@"; do
    case "$arg" in
        --profile=*)   PROFILE="${arg#--profile=}" ;;
        --bench)       BENCH_MODE="full" ;;
        --bench=*)     BENCH_MODE="${arg#--bench=}" ;;
        --no-recorder) RUN_RECORDER=false ;;
    esac
done

# ── Colors ────────────────────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[1;33m'; B='\033[0;36m'; R='\033[0;31m'
BOLD='\033[1m'; X='\033[0m'
log()  { echo -e "\n${BOLD}${B}▶ $*${X}"; }
ok()   { echo -e "  ${G}✓${X} $*"; }
warn() { echo -e "  ${Y}!${X} $*"; }
err()  { echo -e "  ${R}✗${X} $*"; }

DEMO_PID=""
REC_PID=""

# ── Cleanup on exit ───────────────────────────────────────────────────────
cleanup() {
    echo ""
    log "Dừng demo server và recorder..."
    [ -n "$DEMO_PID" ] && kill "$DEMO_PID" 2>/dev/null && ok "Demo server stopped (PID $DEMO_PID)"
    [ -n "$REC_PID"  ] && kill "$REC_PID"  2>/dev/null && ok "Recorder stopped  (PID $REC_PID)"

    # Recorder in summary khi nhận SIGTERM — đợi tối đa 3s
    if [ -n "$REC_PID" ]; then
        sleep 2
    fi
    echo ""
    echo -e "${BOLD}════ vm_run.sh finished ════${X}"
    exit 0
}
trap cleanup SIGINT SIGTERM

# ── 1. Load profile env vars (để recorder detect đúng profile) ───────────
log "Profile: $PROFILE"
ENV_FILE=".env.${PROFILE}"
if [ -f "$ENV_FILE" ]; then
    # Export tất cả biến từ profile file
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE" 2>/dev/null || true
    set +a
    ok "Loaded $ENV_FILE"
else
    warn "$ENV_FILE không tồn tại — dùng env hiện tại"
fi

# ── 2. Start pipeline ─────────────────────────────────────────────────────
log "Khởi động pipeline (bash start.sh --profile=$PROFILE)..."
bash start.sh "--profile=$PROFILE"

# ── 3. Smoke test ─────────────────────────────────────────────────────────
log "Kiểm tra pipeline (smoke test)..."
if bash test_smoke.sh --quick; then
    ok "Pipeline healthy"
else
    warn "Smoke test có lỗi — tiếp tục anyway (xem log trên)"
fi

# ── 4. Setup demo/.env nếu chưa có ────────────────────────────────────────
if [ ! -f demo/.env ] && [ -f demo/.env.example ]; then
    cp demo/.env.example demo/.env
    ok "Tạo demo/.env từ .env.example"
fi

# ── 5. Detect public IP ───────────────────────────────────────────────────
VM_IP=$(curl -sf --max-time 3 http://checkip.amazonaws.com 2>/dev/null \
     || curl -sf --max-time 3 http://ifconfig.me 2>/dev/null \
     || hostname -I 2>/dev/null | awk '{print $1}' \
     || echo "localhost")

DEMO_PORT="${DEMO_PORT:-8888}"

# ── 6. Start demo server (background) ────────────────────────────────────
log "Khởi động demo server (port $DEMO_PORT)..."
cd demo
python3 demo_server.py > /tmp/cdc_demo_server.log 2>&1 &
DEMO_PID=$!
cd ..

# Đợi demo server sẵn sàng
for i in $(seq 1 15); do
    if curl -sf --max-time 1 "http://127.0.0.1:${DEMO_PORT}/api/status" >/dev/null 2>&1; then
        ok "Demo server ready (PID $DEMO_PID)"
        break
    fi
    sleep 1
    if [ "$i" -eq 15 ]; then
        warn "Demo server chưa respond sau 15s — tiếp tục anyway"
        warn "Log: /tmp/cdc_demo_server.log"
    fi
done

# ── 7. Start recorder (background) ────────────────────────────────────────
if $RUN_RECORDER; then
    log "Khởi động recorder..."
    python3 demo/record_demo.py --interval 3 > /tmp/cdc_recorder.log 2>&1 &
    REC_PID=$!
    sleep 1
    if kill -0 "$REC_PID" 2>/dev/null; then
        ok "Recorder running (PID $REC_PID)"
        ok "Output → demo/recordings/"
    else
        warn "Recorder crashed — xem /tmp/cdc_recorder.log"
        REC_PID=""
    fi
fi

# ── 8. Benchmark (nếu yêu cầu) ────────────────────────────────────────────
if [ -n "$BENCH_MODE" ]; then
    log "Chạy benchmark mode: $BENCH_MODE..."
    bash run_bench.sh "$BENCH_MODE"
    ok "Benchmark xong — xem kết quả tại Grafana"
fi

# ── 9. Print access info ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}════════════════════════════════════════════════${X}"
echo -e "${BOLD} CDC Pipeline đang chạy${X}"
echo -e "${BOLD}════════════════════════════════════════════════${X}"
echo ""
echo -e "  ${G}Demo dashboard${X}  : http://${VM_IP}:${DEMO_PORT}"
echo -e "  ${G}Grafana${X}         : http://${VM_IP}:3000  (admin/admin)"
echo -e "  ${G}Prometheus${X}      : http://${VM_IP}:9090"
echo -e "  ${G}Spark UI${X}        : http://${VM_IP}:8080"
echo ""
if $RUN_RECORDER && [ -n "$REC_PID" ]; then
    echo -e "  ${B}Recorder${X}        : PID $REC_PID  →  demo/recordings/"
fi
echo ""
echo -e "  ${Y}Ctrl+C${X} để dừng demo server + recorder"
echo ""
echo -e "${BOLD}════════════════════════════════════════════════${X}"

# ── 10. Wait ──────────────────────────────────────────────────────────────
wait
