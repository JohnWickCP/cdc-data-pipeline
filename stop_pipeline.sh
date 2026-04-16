#!/bin/bash
# ============================================================
# CDC Pipeline — Stop Script
# Dừng pipeline: bash stop_pipeline.sh
# Dừng + xóa data: bash stop_pipeline.sh -v
# ============================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$PROJECT_DIR/pipeline"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
info() { echo -e "${CYAN}[→]${NC} $1"; }

echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  CDC Pipeline — Shutdown${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""

info "Dừng metrics exporter..."
pkill -f "metrics_exporter.py" 2>/dev/null && log "Metrics exporter đã dừng" || log "Metrics exporter không chạy"

cd "$COMPOSE_DIR"

if [ "$1" = "-v" ] || [ "$1" = "--volumes" ]; then
    warn "Xóa volumes (tất cả data sẽ bị mất)..."
    echo ""
    read -p "  Bạn chắc chắn? (y/N): " CONFIRM
    if [ "$CONFIRM" = "y" ] || [ "$CONFIRM" = "Y" ]; then
        docker compose down -v 2>&1 | tail -5
        log "Đã dừng + xóa volumes"
        warn "Lần start tiếp sẽ là cold start"
    else
        docker compose down 2>&1 | tail -5
        log "Đã dừng (giữ volumes)"
    fi
else
    docker compose down 2>&1 | tail -5
    log "Đã dừng containers (giữ data)"
    echo -e "  ${CYAN}Tip: bash stop_pipeline.sh -v để xóa cả data${NC}"
fi

echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "  ${GREEN}✓ Pipeline đã dừng${NC}"
echo -e "  ${CYAN}Khởi động: bash start_full_pipeline.sh${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""
