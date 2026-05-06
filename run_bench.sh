#!/bin/bash

# ============================================================
# Script chạy Benchmark trực tiếp trên nền Docker
# ============================================================

export MSYS_NO_PATHCONV=1

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$PROJECT_DIR/pipeline"

MODE=${1:-quick} # mặc định là quick nếu không truyền tham số

echo "▶ Đang kết nối vào metrics-exporter để chạy benchmark mode: $MODE..."

# Chạy lệnh trong container cdc-metrics-exporter
docker exec -it cdc-metrics-exporter python /app/benchmark/run_benchmark_v4.py $MODE

echo ""
echo "✅ Benchmark đã hoàn tất. Bạn có thể mở Grafana để xem kết quả."
