#!/usr/bin/env python3
"""
metrics_exporter_patch.py
==========================
THÊM vào cuối metrics_exporter.py hiện tại của bạn,
hoặc chạy độc lập song song trên port 8001.

Đọc scalability_report.json và scaling_results.json
rồi expose metrics cho Prometheus/Grafana.

Cách dùng:
    # Option 1: Chạy song song (port khác)
    python3 metrics_exporter_patch.py --port 8001

    # Option 2: Thêm vào metrics_exporter.py hiện tại
    # Copy hàm export_benchmark_metrics() vào và gọi trong vòng lặp chính

Prometheus config cần thêm:
    - job_name: 'cdc-benchmark'
      static_configs:
        - targets: ['host.docker.internal:8001']
"""

import json
import time
import os
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

# ── Đường dẫn file kết quả benchmark ────────────────────────────────
# Điều chỉnh nếu bạn chạy từ thư mục khác
SCALABILITY_JSON = Path(__file__).parent / "scalability_report.json"
SCALING_JSON     = Path(__file__).parent / "scaling_results.json"

# ── Cache để không đọc file liên tục ────────────────────────────────
_cache = {"data": {}, "ts": 0}
CACHE_TTL = 30  # giây


def load_benchmark_data():
    now = time.time()
    if now - _cache["ts"] < CACHE_TTL and _cache["data"]:
        return _cache["data"]

    data = {}

    # Đọc scalability_report.json
    if SCALABILITY_JSON.exists():
        try:
            with open(SCALABILITY_JSON) as f:
                sr = json.load(f)
            data["scalability"] = sr
        except Exception:
            pass

    # Đọc scaling_results.json (partition scaling)
    if SCALING_JSON.exists():
        try:
            with open(SCALING_JSON) as f:
                sc = json.load(f)
            data["scaling"] = sc
        except Exception:
            pass

    _cache["data"] = data
    _cache["ts"]   = now
    return data


def build_metrics(data):
    """Sinh Prometheus text format từ benchmark data."""
    lines = []

    def gauge(name, value, help_text="", labels=""):
        if value is None:
            return
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        if labels:
            lines.append(f"{name}{{{labels}}} {value}")
        else:
            lines.append(f"{name} {value}")

    sr = data.get("scalability", {})

    # ── Latency metrics ──────────────────────────────────────────
    lat = sr.get("latency")
    if lat:
        gauge("cdc_benchmark_latency_p50",
              lat.get("p50_s"),
              "End-to-end latency p50 (seconds)")
        gauge("cdc_benchmark_latency_p95",
              lat.get("p95_s"),
              "End-to-end latency p95 (seconds)")
        gauge("cdc_benchmark_latency_p99",
              lat.get("p99_s"),
              "End-to-end latency p99 (seconds)")
        gauge("cdc_benchmark_latency_avg",
              lat.get("avg_s"),
              "End-to-end latency average (seconds)")
        gauge("cdc_benchmark_latency_min",
              lat.get("min_s"),
              "End-to-end latency min (seconds)")
        gauge("cdc_benchmark_latency_max",
              lat.get("max_s"),
              "End-to-end latency max (seconds)")

    # ── Throughput metrics ───────────────────────────────────────
    tp = sr.get("throughput_baseline")
    if tp:
        gauge("cdc_benchmark_throughput_e2e",
              tp.get("e2e_tps"),
              "End-to-end throughput records/second")
        gauge("cdc_benchmark_throughput_mysql",
              tp.get("mysql_tps"),
              "MySQL insert throughput records/second")
        gauge("cdc_benchmark_sync_rate_pct",
              tp.get("sync_rate_pct"),
              "Percentage of records synced to MongoDB")

    # ── Redis latency ────────────────────────────────────────────
    redis_lat = sr.get("redis_latency")
    if redis_lat:
        p99_vals = [v.get("p99_ms") for v in redis_lat.values() if v.get("p99_ms")]
        if p99_vals:
            gauge("cdc_benchmark_redis_p99_ms",
                  max(p99_vals),
                  "Redis serving layer worst p99 latency (ms)")
        p50_vals = [v.get("p50_ms") for v in redis_lat.values() if v.get("p50_ms")]
        if p50_vals:
            gauge("cdc_benchmark_redis_p50_ms",
                  max(p50_vals),
                  "Redis serving layer worst p50 latency (ms)")

    # ── Config metrics ───────────────────────────────────────────
    cfg = sr.get("config")
    if cfg:
        gauge("cdc_benchmark_kafka_partitions",
              cfg.get("kafka_partitions_orders"),
              "Kafka partitions for orders topic")
        gauge("cdc_benchmark_spark_workers",
              cfg.get("spark_workers"),
              "Active Spark workers")

    # ── Scaling comparison ───────────────────────────────────────
    sc = data.get("scaling", {})
    if sc.get("comparison"):
        cmp = sc["comparison"]
        gauge("cdc_scale_baseline_tps",
              cmp.get("baseline_tps"),
              "Throughput baseline (1 partition)")
        gauge("cdc_scale_scaled_tps",
              cmp.get("scaled_tps"),
              "Throughput after scaling (3 partitions)")
        gauge("cdc_scale_improvement_pct",
              cmp.get("throughput_improvement_pct"),
              "Throughput improvement percentage after scaling")
        gauge("cdc_scale_baseline_p95",
              cmp.get("baseline_p95_s"),
              "Latency p95 baseline (seconds)")
        gauge("cdc_scale_scaled_p95",
              cmp.get("scaled_p95_s"),
              "Latency p95 after scaling (seconds)")

    # ── Partition scaling projections ────────────────────────────
    projections = sr.get("partition_scaling", [])
    if projections:
        lines.append("# HELP cdc_benchmark_projected_tps Projected throughput by partition count")
        lines.append("# TYPE cdc_benchmark_projected_tps gauge")
        for proj in projections:
            p = proj.get("partitions")
            tps = proj.get("est_tps")
            if p is not None and tps is not None:
                lines.append(f'cdc_benchmark_projected_tps{{partitions="{p}"}} {tps}')

    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            data    = load_benchmark_data()
            content = build_metrics(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass  # tắt access log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    print(f"[benchmark-exporter] Listening on :{args.port}/metrics")
    print(f"[benchmark-exporter] Reading: {SCALABILITY_JSON}")
    print(f"[benchmark-exporter] Reading: {SCALING_JSON}")
    print(f"[benchmark-exporter] Press Ctrl+C to stop")

    server = HTTPServer(("0.0.0.0", args.port), MetricsHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[benchmark-exporter] Stopped.")


if __name__ == "__main__":
    main()