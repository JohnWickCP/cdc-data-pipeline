#!/usr/bin/env bash
# CDC Demo Server — startup script
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Copy .env nếu chưa có
if [ ! -f .env ] && [ -f .env.example ]; then
    cp .env.example .env
    echo "[demo] Created .env from .env.example — edit nếu cần đổi host"
fi

# Cài dependencies
echo "[demo] Installing dependencies..."
pip install -r requirements.txt -q

echo "[demo] Starting CDC Demo Server..."
echo "[demo] Open: http://localhost:${DEMO_PORT:-8888}"
echo ""

python3 demo_server.py 2>/dev/null || python demo_server.py
