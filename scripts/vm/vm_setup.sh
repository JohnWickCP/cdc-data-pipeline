#!/usr/bin/env bash
# vm_setup.sh — Cài đặt một lần trên VM mới (Ubuntu 22.04)
# Chạy sau khi git clone, trước khi start.sh
#
# Usage: bash vm_setup.sh
# Thời gian: ~3-5 phút

set -euo pipefail

log()  { echo ""; echo "▶ $*"; }
ok()   { echo "  ✓ $*"; }
warn() { echo "  ! $*"; }

# ── 1. Docker ────────────────────────────────────────────────────────────
log "Kiểm tra Docker..."
if command -v docker &>/dev/null && docker compose version &>/dev/null; then
    ok "Docker đã có: $(docker --version)"
else
    log "Cài Docker Engine..."
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
    ok "Docker đã cài. Áp dụng group ngay trong session này..."
    # Áp dụng group mà không cần logout
    exec sg docker "$0 $*"
fi

# ── 2. Python + pip dependencies ─────────────────────────────────────────
log "Kiểm tra Python..."
PYTHON=""
for cmd in python3 python; do
    if $cmd --version &>/dev/null 2>&1; then
        PYTHON="$cmd"; break
    fi
done

if [ -z "$PYTHON" ]; then
    log "Cài Python3..."
    sudo apt-get update -qq
    sudo apt-get install -y python3 python3-pip
    PYTHON="python3"
fi
ok "Python: $($PYTHON --version)"

log "Cài Python dependencies..."
$PYTHON -m pip install --quiet --break-system-packages \
    flask>=2.3.0 \
    pymysql>=1.1.0 \
    cryptography>=42.0 \
    pymongo>=4.6.0 \
    redis>=5.0.0 \
    kafka-python>=2.0.0 \
    requests>=2.28.0 \
    2>/dev/null || \
$PYTHON -m pip install --quiet \
    flask>=2.3.0 \
    pymysql>=1.1.0 \
    cryptography>=42.0 \
    pymongo>=4.6.0 \
    redis>=5.0.0 \
    kafka-python>=2.0.0 \
    requests>=2.28.0
ok "Python packages installed"

# ── 3. Các tool phụ ─────────────────────────────────────────────────────
log "Kiểm tra netcat (nc)..."
if ! command -v nc &>/dev/null; then
    sudo apt-get install -y netcat-openbsd -qq
    ok "netcat installed"
else
    ok "netcat đã có"
fi

log "Kiểm tra btop (live resource monitor cho demo)..."
if ! command -v btop &>/dev/null; then
    sudo apt-get update -qq && sudo apt-get install -y btop -qq
    ok "btop đã cài"
else
    ok "btop đã có"
fi

# ── 3b. Swap ─────────────────────────────────────────────────────────────
# Cần thiết khi chạy 6 Spark workers trên VM 32GB — tránh OOM nếu GC spike.
log "Kiểm tra swap..."
if swapon --show 2>/dev/null | grep -q .; then
    ok "Swap đã có: $(free -h | awk '/^Swap/{print $2}')"
else
    SWAPFILE=/swapfile
    if [ ! -f "$SWAPFILE" ]; then
        log "Tạo swap 4GB..."
        sudo fallocate -l 4G "$SWAPFILE"
        sudo chmod 600 "$SWAPFILE"
        sudo mkswap "$SWAPFILE" -q
        sudo swapon "$SWAPFILE"
        grep -q "$SWAPFILE" /etc/fstab \
            || echo "$SWAPFILE none swap sw 0 0" | sudo tee -a /etc/fstab > /dev/null
        ok "Swap 4GB đã tạo và kích hoạt (persist qua reboot)"
    else
        sudo swapon "$SWAPFILE" 2>/dev/null && ok "Swap file đã tồn tại, kích hoạt lại" \
            || ok "Swap file đã có và đang active"
    fi
fi

# ── 4. Demo env file ─────────────────────────────────────────────────────
log "Cài đặt demo/.env..."
if [ ! -f demo/.env ]; then
    cp demo/.env.example demo/.env
    ok "Tạo demo/.env từ .env.example (mặc định: localhost)"
else
    ok "demo/.env đã có — giữ nguyên"
fi

# ── 5. Thư mục recordings ────────────────────────────────────────────────
mkdir -p demo/recordings
ok "demo/recordings/ ready"

# ── 6. Phát hiện hardware ────────────────────────────────────────────────
log "Phát hiện hardware..."
DETECTED_PROFILE=$(bash start.sh --detect 2>/dev/null | grep "Profile" | awk '{print $NF}' || echo "server")

echo ""
echo "════════════════════════════════════════════════"
echo " VM Setup hoàn tất!"
echo "════════════════════════════════════════════════"
echo ""
echo " Thông số phát hiện:"
echo "   RAM : $(free -m 2>/dev/null | awk '/^Mem/{printf "%.1f GB", $2/1024}' || echo "?")"
echo "   CPU : $(nproc 2>/dev/null || echo "?") cores"
echo "   Disk: $(df -h / 2>/dev/null | awk 'NR==2{print $4}' || echo "?") available"
echo ""
echo " Bước tiếp theo:"
echo ""
echo "   ▶ Chạy tất cả trong MỘT lệnh:"
echo ""
echo "      bash scripts/vm/vm_run.sh                  # pipeline + demo + recorder"
echo "      bash scripts/vm/vm_run.sh --bench          # + benchmark full"
echo "      bash scripts/vm/vm_run.sh --bench=quick    # + benchmark quick (~3 phút)"
echo ""
echo "   Ctrl+C để dừng — summary tự lưu vào demo/recordings/"
echo ""
echo "   ── Hoặc chạy riêng lẻ nếu cần: ────────────────────────"
echo "      bash start.sh --profile=vm     # pipeline (5-8 phút lần đầu)"
echo "      bash scripts/test_smoke.sh     # kiểm tra"
echo "      cd demo && bash run_demo.sh    # demo server"
echo "      python3 demo/record_demo.py    # recorder"
echo ""
echo "════════════════════════════════════════════════"
