#!/usr/bin/env python3
"""
run_benchmark.py — CDC Pipeline Benchmark Runner v4

Cải tiến so với v3:
- E2E TPS thật: đo từ lúc inject đến lúc MongoDB sync xong (không trick drain)
- Thêm mức 2000 TPS
- Mode partition: tự đổi Kafka partition rồi test lại
- Output JSON chi tiết

Usage:
    python3 benchmark/run_benchmark.py              # full mode (default)
    python3 benchmark/run_benchmark.py quick         # 3 phút
    python3 benchmark/run_benchmark.py stress        # tìm max TPS
    python3 benchmark/run_benchmark.py partition      # test với 3 partitions

Require: pymysql, pymongo, requests, redis, kafka-python
"""

import sys
import os
import time
import json
import argparse
import subprocess
import statistics
import threading
from pathlib import Path
from datetime import datetime

try:
    import pymysql
    import pymongo
    import requests
except ImportError as e:
    print(f"ERROR: Missing dependency - {e}")
    print("Run: pip3 install --break-system-packages pymysql pymongo requests")
    sys.exit(1)

# ══════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "benchmark" else SCRIPT_DIR
RESULTS_DIR = PROJECT_DIR / "benchmark" / "results"

MYSQL_CONFIG = dict(host="127.0.0.1", port=3306, user="root", password="root",
                    database="inventory", autocommit=True, connect_timeout=5)
MONGO_URI = "mongodb://127.0.0.1:27017"
METRICS_URL = "http://localhost:8000/metrics"
SPARK_MASTER_URL = "http://localhost:8080/json/"

START_ID = 1_000_000

# Colors
class C:
    G = '\033[0;32m'; Y = '\033[1;33m'; R = '\033[0;31m'
    B = '\033[0;36m'; BOLD = '\033[1m'; DIM = '\033[2m'; X = '\033[0m'

def ok(m):   print(f"{C.G}✓{C.X} {m}")
def warn(m): print(f"{C.Y}⚠{C.X} {m}")
def err(m):  print(f"{C.R}✗{C.X} {m}")
def info(m): print(f"{C.DIM}  {m}{C.X}")
def step(m): print(f"\n{C.BOLD}{C.B}══ {m} ══{C.X}")


# ══════════════════════════════════════════════════════════
# Metrics
# ══════════════════════════════════════════════════════════
def get_all_metrics():
    try:
        resp = requests.get(METRICS_URL, timeout=2)
        metrics = {}
        for line in resp.text.split('\n'):
            if line and not line.startswith('#'):
                parts = line.split()
                if len(parts) == 2:
                    try:
                        metrics[parts[0]] = float(parts[1])
                    except ValueError:
                        pass
        return metrics
    except Exception:
        return {}


def sample_metrics():
    m = get_all_metrics()
    return {
        'mysql_rate':  m.get('cdc_mysql_insert_rate', 0),
        'kafka_rate':  m.get('cdc_kafka_rate_total', 0),
        'spark_ms':    m.get('cdc_spark_batch_duration_ms', 0),
        'mongo_rate':  m.get('cdc_mongo_write_rate', 0),
        'lag':         m.get('cdc_lag_total', 0),
        'mysql_count': m.get('cdc_mysql_customers_total', 0),
        'mongo_count': m.get('cdc_mongo_customers_total', 0),
        'batches':     m.get('cdc_spark_batches_total', 0),
        'spark_cores': m.get('cdc_spark_executor_cores', 0),
        'spark_mem':   m.get('cdc_spark_executor_memory_mb', 0),
    }


# ══════════════════════════════════════════════════════════
# Inject — batch INSERT cho tốc độ cao
# ══════════════════════════════════════════════════════════
def inject_load(target_tps, duration_s, batch_size=10):
    """
    Inject records vào MySQL ở target_tps trong duration_s giây.
    Dùng batch INSERT (nhiều VALUES trong 1 query) để giảm overhead.
    Returns: (records_inserted, actual_inject_seconds)
    """
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    total = target_tps * duration_s
    interval = 1.0 / (target_tps / batch_size)  # sleep giữa các batch

    inserted = 0
    start = time.time()

    for i in range(total // batch_size):
        tick = time.time()
        values = ",".join([
            f"({START_ID + inserted + j}, 'B{inserted+j}', 'b{inserted+j}@t.com', '09{(inserted+j)%1000000:06d}')"
            for j in range(batch_size)
        ])
        try:
            cur.execute(
                f"INSERT INTO customers (id, name, email, phone) VALUES {values} "
                f"ON DUPLICATE KEY UPDATE name=VALUES(name)"
            )
            inserted += batch_size
        except Exception as e:
            print(f"  [INSERT ERROR] {e}")
            break

        sleep_t = interval - (time.time() - tick)
        if sleep_t > 0:
            time.sleep(sleep_t)

    elapsed = time.time() - start
    conn.close()
    return inserted, elapsed


# ══════════════════════════════════════════════════════════
# E2E Test — đo THẬT (inject + chờ sync, tính tổng thời gian)
# ══════════════════════════════════════════════════════════
def run_e2e_test(target_tps, duration_s=30, max_drain_s=120):
    """
    Đo E2E TPS thật:
    1. Inject records ở target_tps trong duration_s
    2. Chờ MongoDB sync xong (hoặc timeout)
    3. E2E TPS = mongo_delta / TỔNG thời gian (inject + drain)

    Returns dict với kết quả chi tiết
    """
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)["inventory"]

    # Baseline
    cur.execute("SELECT COUNT(*) FROM customers")
    before_mysql = cur.fetchone()[0]
    before_mongo = mongo["customers"].count_documents({})

    # Sample metrics trong khi inject
    samples = []
    stop_sampling = threading.Event()

    def sampler():
        while not stop_sampling.is_set():
            samples.append(sample_metrics())
            time.sleep(2)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()

    # Inject
    t_start = time.time()
    inserted, inject_elapsed = inject_load(target_tps, duration_s)
    inject_tps = inserted / inject_elapsed if inject_elapsed > 0 else 0

    # Đếm MySQL sau inject
    cur.execute("SELECT COUNT(*) FROM customers")
    after_mysql = cur.fetchone()[0]
    mysql_delta = after_mysql - before_mysql

    # Chờ MongoDB sync XONG (đếm cho đến khi = MySQL)
    drain_start = time.time()
    synced = False
    while time.time() - t_start < duration_s + max_drain_s:
        mongo_count = mongo["customers"].count_documents({})
        if mongo_count >= after_mysql:
            synced = True
            break
        time.sleep(0.5)

    t_end = time.time()
    total_elapsed = t_end - t_start
    drain_elapsed = t_end - drain_start if drain_start else 0

    stop_sampling.set()
    sampler_thread.join(timeout=3)

    # Final counts
    final_mongo = mongo["customers"].count_documents({})
    mongo_delta = final_mongo - before_mongo
    lag = mysql_delta - mongo_delta

    # E2E TPS = records thực sự đến MongoDB / TỔNG thời gian
    e2e_tps = mongo_delta / total_elapsed if total_elapsed > 0 else 0

    # Stats từ samples
    spark_values = sorted([s['spark_ms'] for s in samples if s['spark_ms'] > 0])
    kafka_rates = [s['kafka_rate'] for s in samples if s['kafka_rate'] > 0]
    mongo_rates = [s['mongo_rate'] for s in samples if s['mongo_rate'] > 0]

    result = {
        "target_tps": target_tps,
        "duration_s": duration_s,
        "records_inserted": inserted,
        "mysql_delta": mysql_delta,
        "mongo_delta": mongo_delta,
        "lag_remaining": lag,
        "synced": synced,
        "inject_elapsed_s": round(inject_elapsed, 1),
        "drain_elapsed_s": round(drain_elapsed, 1),
        "total_elapsed_s": round(total_elapsed, 1),
        "inject_tps": round(inject_tps, 1),
        "e2e_tps": round(e2e_tps, 1),
        "spark_batch_avg_ms": round(statistics.mean(spark_values), 1) if spark_values else 0,
        "spark_batch_p95_ms": round(spark_values[int(len(spark_values) * 0.95)], 1) if spark_values else 0,
        "kafka_rate_avg": round(statistics.mean(kafka_rates), 1) if kafka_rates else 0,
        "mongo_rate_avg": round(statistics.mean(mongo_rates), 1) if mongo_rates else 0,
        "samples_count": len(samples),
    }

    conn.close()
    return result


# ══════════════════════════════════════════════════════════
# Cleanup
# ══════════════════════════════════════════════════════════
def cleanup():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM customers WHERE id >= {START_ID}")
        conn.close()
        # Chờ CDC xử lý DELETE
        time.sleep(5)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════
# Hardware info
# ══════════════════════════════════════════════════════════
def get_hardware():
    def run(cmd):
        try:
            return subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode().strip()
        except Exception:
            return "N/A"

    return {
        "cpu_model": run("lscpu | grep 'Model name' | sed 's/.*:\\s*//'"),
        "cpu_cores": int(run("nproc") or "0"),
        "ram_gb": int(run("free -g | awk '/^Mem:/{print $2}'") or "0"),
        "disk_free": run(f"df -h {PROJECT_DIR} | tail -1 | awk '{{print $4}}'"),
    }


# ══════════════════════════════════════════════════════════
# Partition mode
# ══════════════════════════════════════════════════════════
def change_partitions(num_partitions):
    """Đổi số partition cho Kafka topics."""
    topics = [
        "inventory.inventory.customers",
        "inventory.inventory.orders"
    ]
    changed = False
    for topic in topics:
        try:
            result = subprocess.run(
                ["docker", "exec", "cdc-kafka", "kafka-topics",
                 "--alter", "--bootstrap-server", "localhost:9092",
                 "--topic", topic, "--partitions", str(num_partitions)],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                info(f"  {topic} → {num_partitions} partitions")
                changed = True
            else:
                warn(f"  {topic}: {result.stderr.strip()}")
        except Exception as e:
            warn(f"  Lỗi đổi partition {topic}: {e}")

    if changed:
        info("Đợi 10s cho Kafka rebalance...")
        time.sleep(10)
    return changed


def get_current_partitions():
    """Lấy số partition hiện tại."""
    try:
        result = subprocess.run(
            ["docker", "exec", "cdc-kafka", "kafka-topics",
             "--describe", "--bootstrap-server", "localhost:9092",
             "--topic", "inventory.inventory.customers"],
            capture_output=True, text=True, timeout=10
        )
        for line in result.stdout.split('\n'):
            if 'PartitionCount' in line:
                for part in line.split('\t'):
                    if 'PartitionCount' in part:
                        return int(part.split(':')[1].strip())
        return 1
    except Exception:
        return 1


# ══════════════════════════════════════════════════════════
# Pre-flight
# ══════════════════════════════════════════════════════════
def preflight():
    step("Kiểm tra hệ thống")

    try:
        r = requests.get(METRICS_URL, timeout=2)
        assert r.status_code == 200
        ok("Metrics exporter OK")
    except Exception:
        err(f"Metrics exporter không chạy ({METRICS_URL})")
        sys.exit(1)

    try:
        r = requests.get(SPARK_MASTER_URL, timeout=2)
        active = len(r.json().get('activeapps', []))
        if active == 0:
            err("Không có Spark app đang chạy")
            sys.exit(1)
        ok(f"Spark app active ({active})")
    except Exception:
        err("Spark master không truy cập được")
        sys.exit(1)

    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            count = cur.fetchone()[0]
        conn.close()
        ok(f"MySQL OK (customers={count})")
    except Exception:
        err("MySQL connect failed")
        sys.exit(1)


# ══════════════════════════════════════════════════════════
# Mode configs
# ══════════════════════════════════════════════════════════
MODES = {
    'quick': {
        'levels': [100, 200, 500],
        'duration': 20,
        'sustained_duration': 30,
        'description': '~3 phút, demo nhanh',
    },
    'full': {
        'levels': [100, 200, 500, 1000, 2000],
        'duration': 30,
        'sustained_duration': 60,
        'description': '~10 phút, chuẩn báo cáo',
    },
    'stress': {
        'levels': [100, 200, 500, 1000, 2000, 5000],
        'duration': 30,
        'sustained_duration': 60,
        'description': 'tăng đến bottleneck',
    },
    'partition': {
        'levels': [100, 500, 1000, 2000],
        'duration': 30,
        'sustained_duration': 60,
        'partitions': 3,
        'description': 'test với 3 partitions',
    },
}


# ══════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="CDC Benchmark v4 — E2E TPS thật")
    parser.add_argument('mode', nargs='?', default='full', choices=list(MODES.keys()),
                        help='quick | full | stress | partition')
    args = parser.parse_args()

    cfg = MODES[args.mode]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / f"run_{timestamp}.json"

    print(f"{C.BOLD}{C.B}")
    print("╔═══════════════════════════════════════════════════════╗")
    print("║    CDC Pipeline — Benchmark v4 (E2E TPS thật)       ║")
    print("╚═══════════════════════════════════════════════════════╝")
    print(C.X)

    info(f"Mode:        {C.BOLD}{args.mode}{C.X} — {cfg['description']}")
    info(f"Mức test:    {cfg['levels']}")
    info(f"Mỗi mức:     {cfg['duration']}s")
    info(f"Output:      {result_file}")

    preflight()

    # Hardware
    step("Phần cứng")
    hw = get_hardware()
    info(f"CPU:  {hw['cpu_model']}")
    info(f"Cores:{hw['cpu_cores']} | RAM: {hw['ram_gb']}GB | Disk: {hw['disk_free']}")

    # Current partitions
    current_partitions = get_current_partitions()
    info(f"Kafka partitions hiện tại: {current_partitions}")

    # Partition mode: đổi partition
    if args.mode == 'partition':
        target_p = cfg.get('partitions', 3)
        step(f"Đổi Kafka partitions → {target_p}")
        if current_partitions < target_p:
            change_partitions(target_p)
            current_partitions = get_current_partitions()
            ok(f"Partitions hiện tại: {current_partitions}")
        else:
            info(f"Đã có {current_partitions} partitions (≥ {target_p}), không cần đổi")

    # Spark info
    sm = sample_metrics()
    spark_info = {
        "executor_cores": int(sm.get('spark_cores', 0)),
        "executor_memory_mb": int(sm.get('spark_mem', 0)),
        "kafka_partitions": current_partitions,
    }
    info(f"Spark cores: {spark_info['executor_cores']}, Memory: {spark_info['executor_memory_mb']}MB")

    # Warmup
    step("Khởi động nóng (10s, 5 TPS)")
    cleanup()
    inject_load(5, 10)
    time.sleep(10)
    cleanup()
    ok("Xong")

    # ── Ramp-up test ─────────────────────────────────────
    step("Đo E2E TPS thật — tăng dần tải")

    ramp_results = []
    max_e2e_tps = 0
    bottleneck = None

    for target in cfg['levels']:
        print()
        info(f"Mức: {C.BOLD}{target} TPS{C.X} × {cfg['duration']}s")

        cleanup()
        time.sleep(3)

        result = run_e2e_test(target, cfg['duration'])

        # Print
        print(f"  {C.DIM}Inject TPS:{C.X}       {result['inject_tps']}")
        print(f"  {C.DIM}E2E TPS thật:{C.X}     {C.BOLD}{result['e2e_tps']}{C.X}  ← con số thật")
        print(f"  {C.DIM}MySQL delta:{C.X}       {result['mysql_delta']}")
        print(f"  {C.DIM}Mongo delta:{C.X}       {result['mongo_delta']}")
        print(f"  {C.DIM}Lag còn lại:{C.X}       {result['lag_remaining']}")
        print(f"  {C.DIM}Thời gian inject:{C.X}  {result['inject_elapsed_s']}s")
        print(f"  {C.DIM}Thời gian drain:{C.X}   {result['drain_elapsed_s']}s")
        print(f"  {C.DIM}TỔNG thời gian:{C.X}    {result['total_elapsed_s']}s")
        print(f"  {C.DIM}Spark batch:{C.X}        avg {result['spark_batch_avg_ms']}ms, p95 {result['spark_batch_p95_ms']}ms")
        print(f"  {C.DIM}Kafka rate:{C.X}         {result['kafka_rate_avg']} ev/s")

        ramp_results.append(result)

        # Bottleneck?
        if result['lag_remaining'] > 0 or not result['synced']:
            warn(f"BOTTLENECK tại {target} TPS → E2E thật chỉ {result['e2e_tps']} ev/s")
            if result['lag_remaining'] > 0:
                warn(f"  Còn {result['lag_remaining']} records chưa sync")
            if not result['synced']:
                warn(f"  Timeout: MongoDB không sync kịp trong {cfg['duration']}s + drain")

            # Xác định stage nghẽn
            stage = "unknown"
            if result['inject_tps'] < target * 0.7:
                stage = "mysql_inject"
            elif result['kafka_rate_avg'] > 0 and result['kafka_rate_avg'] < result['inject_tps'] * 0.7:
                stage = "debezium_or_kafka"
            else:
                stage = "spark_or_mongo"

            bottleneck = {
                "at_tps": target,
                "e2e_tps": result['e2e_tps'],
                "stage": stage,
                "lag": result['lag_remaining'],
            }
            break
        else:
            ok(f"E2E TPS: {result['e2e_tps']} — pipeline kịp xử lý")
            max_e2e_tps = max(max_e2e_tps, result['e2e_tps'])

    # ── Sustained test ────────────────────────────────────
    sustained_target = max(50, int(max_e2e_tps * 0.8))
    step(f"Chạy ổn định {sustained_target} TPS × {cfg['sustained_duration']}s")

    cleanup()
    time.sleep(3)

    sustained_result = run_e2e_test(sustained_target, cfg['sustained_duration'])

    print(f"  {C.DIM}E2E TPS thật:{C.X}     {C.BOLD}{sustained_result['e2e_tps']}{C.X}")
    print(f"  {C.DIM}Records:{C.X}           {sustained_result['mongo_delta']}")
    print(f"  {C.DIM}Lag:{C.X}               {sustained_result['lag_remaining']}")
    print(f"  {C.DIM}Tổng thời gian:{C.X}    {sustained_result['total_elapsed_s']}s")
    print(f"  {C.DIM}Spark p95:{C.X}          {sustained_result['spark_batch_p95_ms']}ms")

    # ── Generate report ───────────────────────────────────
    step("Lưu kết quả")

    report = {
        "run_id": timestamp,
        "mode": args.mode,
        "version": "v4_e2e_honest",
        "timestamp": datetime.now().isoformat(),

        "config": {
            "tps_levels": cfg['levels'],
            "duration_per_level_s": cfg['duration'],
            "sustained_duration_s": cfg['sustained_duration'],
            "sustained_target_tps": sustained_target,
            "kafka_partitions": current_partitions,
        },

        "hardware": hw,

        "spark_cluster": spark_info,

        "ramp_up": ramp_results,

        "sustained": sustained_result,

        "summary": {
            "max_e2e_tps": max_e2e_tps,
            "bottleneck_detected": bottleneck is not None,
            "bottleneck": bottleneck,
            "measurement_method": "E2E: tổng records đến MongoDB / tổng thời gian (inject + drain). Không trick.",
        },
    }

    with open(result_file, 'w') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    ok(f"Đã lưu: {result_file}")

    # ── Summary ───────────────────────────────────────────
    step("KẾT QUẢ")

    print(f"\n  🎯 Max E2E TPS thật:    {C.BOLD}{max_e2e_tps}{C.X}")

    if bottleneck:
        print(f"  🚧 Bottleneck tại:      {bottleneck['at_tps']} TPS (inject)")
        print(f"  🎯 E2E thật khi nghẽn:  {bottleneck['e2e_tps']} ev/s")
        print(f"  📍 Tầng nghẽn:          {bottleneck['stage']}")
    else:
        print(f"  ✅ Không bottleneck: pipeline kịp xử lý tất cả mức test")

    sr = report['sustained']
    print(f"\n  📊 Chạy ổn định ({sustained_target} TPS × {cfg['sustained_duration']}s):")
    print(f"      E2E TPS thật:     {sr['e2e_tps']}")
    print(f"      Records đến Mongo: {sr['mongo_delta']}")
    print(f"      Lag còn lại:      {sr['lag_remaining']}")
    print(f"      Spark p50/p95:    {sr['spark_batch_avg_ms']}/{sr['spark_batch_p95_ms']}ms")
    print(f"      Kafka partitions: {current_partitions}")

    print(f"\n  📋 Cách đo: E2E = records đến MongoDB ÷ TỔNG thời gian (inject + drain)")
    print(f"  📂 Chi tiết: {result_file}")

    # Cleanup
    step("Dọn dẹp")
    cleanup()
    ok("Xong")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹  Dừng bởi người dùng")
        cleanup()
        sys.exit(1)
