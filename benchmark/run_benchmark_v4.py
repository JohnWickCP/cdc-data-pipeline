#!/usr/bin/env python3
"""
run_benchmark.py — CDC Pipeline Benchmark Runner v4

Cải tiến so với v3:
- E2E records/s thật: đo từ lúc inject đến lúc MongoDB sync xong (không trick drain)
- Thêm mức 2000 records/s
- Mode partition: tự đổi Kafka partition rồi test lại
- Output JSON chi tiết

Usage:
    python3 benchmark/run_benchmark.py              # full mode (default)
    python3 benchmark/run_benchmark.py quick         # 3 phút
    python3 benchmark/run_benchmark.py stress        # tìm max records/s
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

MYSQL_CONFIG = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", 3306)),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", "root"),
    database=os.environ.get("MYSQL_DB", "inventory"),
    autocommit=True,
    connect_timeout=5
)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
METRICS_URL = os.environ.get("METRICS_URL", "http://localhost:8000/metrics")
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "http://cdc-spark-master:8080/json/")

START_ID     = 1_000_000
PROBE_ID_BASE = 900_000   # Probe IDs for latency measurement (< START_ID, not cleaned by cleanup())

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


def inject_realistic_load(target_tps, duration_s, batch_size=10):
    """
    Inject workload realistic: 60% INSERT / 30% UPDATE / 10% DELETE.
    Phản ánh traffic production thực tế hơn thuần INSERT.
    Returns: (total_events, elapsed_s, breakdown_dict)
    """
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    total_batches = max(1, (target_tps * duration_s) // batch_size)
    interval = 1.0 / (target_tps / batch_size)

    ins_per_batch = max(1, int(batch_size * 0.6))
    upd_per_batch = max(1, int(batch_size * 0.3))
    del_per_batch = batch_size - ins_per_batch - upd_per_batch

    # Lấy ID seed để UPDATE (dùng data gốc, không xóa)
    cur.execute("SELECT id FROM customers WHERE id < 1000000 LIMIT 100")
    seed_ids = [row[0] for row in cur.fetchall()] or [1, 2, 3]

    total_ins = total_upd = total_del = 0
    start = time.time()

    for i in range(total_batches):
        tick = time.time()

        # INSERT mới
        vals = ",".join([
            f"({START_ID + total_ins + j}, 'Mix{total_ins+j}', 'm{total_ins+j}@t.com', '09{(total_ins+j)%1000000:06d}')"
            for j in range(ins_per_batch)
        ])
        try:
            cur.execute(
                f"INSERT INTO customers (id, name, email, phone) VALUES {vals} "
                f"ON DUPLICATE KEY UPDATE name=VALUES(name)"
            )
            total_ins += ins_per_batch
        except Exception:
            pass

        # UPDATE seed records (cycling)
        for j in range(upd_per_batch):
            uid = seed_ids[(i * upd_per_batch + j) % len(seed_ids)]
            try:
                cur.execute(f"UPDATE customers SET phone='09{(total_upd)%1000000:06d}' WHERE id={uid}")
                total_upd += 1
            except Exception:
                pass

        # DELETE vài record vừa INSERT
        if del_per_batch > 0 and total_ins > del_per_batch * 10:
            del_id = START_ID + total_del * 7
            try:
                cur.execute(f"DELETE FROM customers WHERE id={del_id} AND id >= {START_ID}")
                total_del += 1
            except Exception:
                pass

        sleep_t = interval - (time.time() - tick)
        if sleep_t > 0:
            time.sleep(sleep_t)

    elapsed = time.time() - start
    conn.close()
    return total_ins + total_upd + total_del, elapsed, {
        'insert': total_ins, 'update': total_upd, 'delete': total_del
    }


def get_kafka_offset():
    """Lấy tổng Kafka offset từ metrics exporter."""
    m = get_all_metrics()
    return m.get('cdc_kafka_customers_offset', 0) + m.get('cdc_kafka_orders_offset', 0)


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

    # Snapshot MongoDB ngay khi inject xong → đo lag tích lũy trong giai đoạn inject
    mongo_at_inject_end = mongo["customers"].count_documents({})
    lag_at_inject_end = max(0, mysql_delta - (mongo_at_inject_end - before_mongo))

    # Chờ MongoDB sync XONG: phải tăng thêm đúng mysql_delta từ baseline
    target_mongo = before_mongo + mysql_delta
    drain_start = time.time()
    synced = False
    drain_timeline = []   # [{t_s, lag}] sampled mỗi 5s để thấy lag giảm thế nào
    _next_sample_t = drain_start  # sample ngay t=0

    while time.time() - t_start < duration_s + max_drain_s:
        now = time.time()
        mongo_count = mongo["customers"].count_documents({})

        if now >= _next_sample_t:
            drain_timeline.append({
                "t_s": round(now - drain_start, 1),
                "lag": max(0, target_mongo - mongo_count),
            })
            _next_sample_t = now + 5.0

        if mongo_count >= target_mongo:
            synced = True
            break
        time.sleep(1.0)

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

    # Drain rate = records MongoDB nhận được trong giai đoạn drain / thời gian drain
    drained_during_drain = final_mongo - mongo_at_inject_end
    drain_rate_rps = round(drained_during_drain / drain_elapsed, 1) if drain_elapsed > 0 else 0

    # Stats từ samples
    spark_values = sorted([s['spark_ms'] for s in samples if s['spark_ms'] > 0])
    kafka_rates = [s['kafka_rate'] for s in samples if s['kafka_rate'] > 0]
    mongo_rates = [s['mongo_rate'] for s in samples if s['mongo_rate'] > 0]
    lag_values = [s['lag'] for s in samples if s.get('lag', 0) > 0]
    peak_lag = round(max(lag_values, default=0))

    result = {
        "target_tps": target_tps,
        "duration_s": duration_s,
        "records_inserted": inserted,
        "mysql_delta": mysql_delta,
        "mongo_delta": mongo_delta,
        "lag_remaining": lag,
        "lag_at_inject_end": lag_at_inject_end,
        "peak_lag": peak_lag,
        "drain_rate_rps": drain_rate_rps,
        "synced": synced,
        "inject_elapsed_s": round(inject_elapsed, 1),
        "drain_elapsed_s": round(drain_elapsed, 1),
        "total_elapsed_s": round(total_elapsed, 1),
        "inject_tps": round(inject_tps, 1),
        "e2e_tps": round(e2e_tps, 1),
        "spark_batch_avg_ms": round(statistics.mean(spark_values), 1) if spark_values else 0,
        "spark_batch_p50_ms": round(spark_values[int(len(spark_values) * 0.50)], 1) if spark_values else 0,
        "spark_batch_p95_ms": round(spark_values[int(len(spark_values) * 0.95)], 1) if spark_values else 0,
        "spark_batch_p99_ms": round(spark_values[int(len(spark_values) * 0.99)], 1) if spark_values else 0,
        "kafka_rate_avg": round(statistics.mean(kafka_rates), 1) if kafka_rates else 0,
        "mongo_rate_avg": round(statistics.mean(mongo_rates), 1) if mongo_rates else 0,
        "samples_count": len(samples),
    }

    conn.close()
    return result


def run_e2e_realistic_test(target_tps, duration_s=30, max_drain_s=120):
    """
    Đo E2E cho workload realistic (INSERT/UPDATE/DELETE mix).
    Dùng Kafka offset delta thay vì MongoDB count (DELETE làm giảm count).
    E2E events/s = kafka_delta / TỔNG thời gian.
    """
    before_kafka = get_kafka_offset()

    samples = []
    stop_sampling = threading.Event()

    def sampler():
        while not stop_sampling.is_set():
            samples.append(sample_metrics())
            time.sleep(2)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()

    t_start = time.time()
    total_events, inject_elapsed, breakdown = inject_realistic_load(target_tps, duration_s)
    inject_rate = total_events / inject_elapsed if inject_elapsed > 0 else 0

    # Chờ Kafka offset ổn định (không tăng nữa = drain xong)
    drain_start = time.time()
    prev_offset = get_kafka_offset()
    settled = False
    while time.time() - t_start < duration_s + max_drain_s:
        time.sleep(2)
        cur_offset = get_kafka_offset()
        if cur_offset == prev_offset:
            settled = True
            break
        prev_offset = cur_offset

    t_end = time.time()
    total_elapsed = t_end - t_start
    drain_elapsed = t_end - drain_start

    stop_sampling.set()
    sampler_thread.join(timeout=3)

    after_kafka = get_kafka_offset()
    kafka_delta = after_kafka - before_kafka
    e2e_rate = kafka_delta / total_elapsed if total_elapsed > 0 else 0

    spark_values = sorted([s['spark_ms'] for s in samples if s['spark_ms'] > 0])
    kafka_rates = [s['kafka_rate'] for s in samples if s['kafka_rate'] > 0]

    return {
        "target_tps":         target_tps,
        "duration_s":         duration_s,
        "total_events":       total_events,
        "breakdown":          breakdown,
        "kafka_delta":        int(kafka_delta),
        "inject_elapsed_s":   round(inject_elapsed, 1),
        "drain_elapsed_s":    round(drain_elapsed, 1),
        "total_elapsed_s":    round(total_elapsed, 1),
        "inject_rate":        round(inject_rate, 1),
        "e2e_tps":            round(e2e_rate, 1),
        "lag_remaining":      0,
        "synced":             settled,
        "spark_batch_avg_ms": round(statistics.mean(spark_values), 1) if spark_values else 0,
        "spark_batch_p50_ms": round(spark_values[int(len(spark_values) * 0.50)], 1) if spark_values else 0,
        "spark_batch_p95_ms": round(spark_values[int(len(spark_values) * 0.95)], 1) if spark_values else 0,
        "spark_batch_p99_ms": round(spark_values[int(len(spark_values) * 0.99)], 1) if spark_values else 0,
        "kafka_rate_avg":     round(statistics.mean(kafka_rates), 1) if kafka_rates else 0,
        "samples_count":      len(samples),
    }


# ══════════════════════════════════════════════════════════
# Cleanup
# ══════════════════════════════════════════════════════════
def cleanup():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM customers WHERE id >= {START_ID}")
        conn.close()
        # Chờ CDC xử lý DELETE — phải đợi MongoDB drain về ~3 (original records)
        time.sleep(5)
        try:
            mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)["inventory"]
            deadline = time.time() + 120  # max 2 phút wait
            while time.time() < deadline:
                count = mongo["customers"].count_documents({})
                if count <= 10:  # về gần 3 original records
                    break
                time.sleep(3)
        except Exception:
            time.sleep(10)  # fallback nếu không kết nối được MongoDB
    except Exception:
        pass


# ══════════════════════════════════════════════════════════
# E2E Latency — per-record P50/P95/P99
# ══════════════════════════════════════════════════════════
def measure_e2e_latency(n_probes=20):
    """
    Đo E2E latency per-record: MySQL INSERT → MongoDB document available.
    Dùng PROBE_ID_BASE (900_000) để tránh conflict với benchmark records (1_000_000+).
    Returns dict với P50/P95/P99 (ms), hoặc None nếu pipeline không hoạt động.
    """
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cur = conn.cursor()
        mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)["inventory"]
    except Exception as e:
        warn(f"  Không kết nối được MySQL/MongoDB để đo latency: {e}")
        return None

    # Xóa probe records cũ (cả MySQL lẫn MongoDB trực tiếp để không chờ CDC)
    try:
        cur.execute(
            f"DELETE FROM customers WHERE id >= {PROBE_ID_BASE} AND id < {PROBE_ID_BASE + n_probes}"
        )
        for i in range(n_probes):
            mongo["customers"].delete_one({"_id": PROBE_ID_BASE + i})
        time.sleep(1)
    except Exception:
        pass

    latencies = []
    timeout_ms = 12_000  # 12s > 2× trigger interval (5s)

    info(f"  Đang probe {n_probes} records (mỗi record chờ tối đa {timeout_ms}ms)...")

    for i in range(n_probes):
        probe_id = PROBE_ID_BASE + i

        try:
            t0 = time.time()
            cur.execute(
                f"INSERT INTO customers (id, name, email, phone) "
                f"VALUES ({probe_id}, 'Probe{i}', 'probe{i}@bench.test', '0900000000') "
                f"ON DUPLICATE KEY UPDATE name=VALUES(name)"
            )
        except Exception as e:
            warn(f"  Probe {i}: INSERT lỗi — {e}")
            continue

        # Poll MongoDB 50ms/lần
        found = False
        while (time.time() - t0) * 1000 < timeout_ms:
            try:
                if mongo["customers"].find_one({"_id": probe_id}):
                    latencies.append((time.time() - t0) * 1000)
                    found = True
                    break
            except Exception:
                pass
            time.sleep(0.05)

        if not found:
            warn(f"  Probe {i}: timeout {timeout_ms}ms — record không đến MongoDB (pipeline đang bận?)")

        time.sleep(0.1)  # Tránh dồn nhiều probe vào cùng batch

    # Cleanup probe records
    try:
        cur.execute(
            f"DELETE FROM customers WHERE id >= {PROBE_ID_BASE} AND id < {PROBE_ID_BASE + n_probes}"
        )
    except Exception:
        pass
    conn.close()

    if not latencies:
        err("  Không đo được latency nào — kiểm tra Spark job có đang chạy không")
        return None

    s = sorted(latencies)
    n = len(s)

    def pct(data, p):
        idx = max(0, min(int(len(data) * p / 100), len(data) - 1))
        return round(data[idx], 1)

    result = {
        "n_probes":   n_probes,
        "n_measured": n,
        "p50_ms":  pct(s, 50),
        "p95_ms":  pct(s, 95),
        "p99_ms":  pct(s, 99),
        "avg_ms":  round(statistics.mean(s), 1),
        "min_ms":  round(s[0], 1),
        "max_ms":  round(s[-1], 1),
    }

    ok(f"  E2E Latency (n={n}/{n_probes}):  "
       f"P50={result['p50_ms']}ms  P95={result['p95_ms']}ms  P99={result['p99_ms']}ms  "
       f"avg={result['avg_ms']}ms")
    return result


# ══════════════════════════════════════════════════════════
# Hardware info
# ══════════════════════════════════════════════════════════
def get_hardware():
    def run(cmd):
        try:
            return subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode().strip()
        except Exception:
            return "N/A"

    def ram_gb():
        try:
            # /proc/meminfo always works inside Docker (reflects host RAM)
            line = open('/proc/meminfo').readline()  # "MemTotal:   16229552 kB"
            return round(int(line.split()[1]) / 1024 / 1024, 1)
        except Exception:
            return 0.0

    return {
        "cpu_model": run("lscpu | grep 'Model name' | sed 's/.*:\\s*//'"),
        "cpu_cores": int(run("nproc") or "0"),
        "ram_gb": ram_gb(),
        "disk_free": run(f"df -h {PROJECT_DIR} | tail -1 | awk '{{print $4}}'"),
    }


def detect_spark_engine():
    """Detect Scala JAR vs PySpark via Spark Master app name."""
    urls = [SPARK_MASTER_URL, "http://localhost:8080/json/"]
    for url in urls:
        try:
            import urllib.request, json
            with urllib.request.urlopen(url, timeout=3) as r:
                data = json.loads(r.read())
            apps = data.get("activeapps", [])
            if not apps:
                continue
            name = apps[0].get("name", "")
            if "CDC-MySQL-To-MongoDB-Redis" in name:
                return "scala"
            elif "Pipeline" in name or "Python" in name or "python" in name.lower():
                return "python"
        except Exception:
            continue
    return "unknown"


def get_spark_worker_count():
    """Lấy số Spark worker đang alive từ Spark Master API."""
    import urllib.request, json as _json
    urls = [SPARK_MASTER_URL, "http://localhost:8080/json/"]
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                data = _json.loads(r.read())
            alive = [w for w in data.get("workers", []) if w.get("state") == "ALIVE"]
            if alive:
                return len(alive)
        except Exception:
            continue
    return 0


# ══════════════════════════════════════════════════════════
# Partition mode
# ══════════════════════════════════════════════════════════
def change_partitions(num_partitions):
    """Đổi số partition cho Kafka topics."""
    changed = change_partitions_via_kafka(num_partitions)
    if changed:
        # 60s: Kafka Structured Streaming consumer group rebalance thường mất 20-60s.
        # 10s quá ngắn → benchmark đo trong khi Spark đang rebalance → TPS ảo thấp.
        info("Đợi 60s cho Kafka rebalance + Spark partition discovery...")
        time.sleep(60)
    return changed


def get_current_partitions():
    """Lấy số partition hiện tại qua kafka-python (không cần docker exec)."""
    from kafka import KafkaConsumer
    for servers in ['localhost:9092', 'cdc-kafka:9092']:
        try:
            consumer = KafkaConsumer(bootstrap_servers=servers,
                                     request_timeout_ms=5000,
                                     api_version_auto_timeout_ms=3000)
            partitions = consumer.partitions_for_topic('inventory.inventory.customers')
            consumer.close()
            return len(partitions) if partitions else 1
        except Exception:
            continue
    return 1


def change_partitions_via_kafka(num_partitions):
    """Tăng số partition qua kafka-python AdminClient (không cần docker exec)."""
    from kafka.admin import KafkaAdminClient, NewPartitions
    for servers in ['localhost:9092', 'cdc-kafka:9092']:
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers,
                                     request_timeout_ms=10000)
            current = get_current_partitions()
            if current >= num_partitions:
                admin.close()
                return True
            topics = {
                'inventory.inventory.customers': NewPartitions(total_count=num_partitions),
                'inventory.inventory.orders':    NewPartitions(total_count=num_partitions),
            }
            admin.create_partitions(topics)
            admin.close()
            time.sleep(5)
            return True
        except Exception as e:
            warn(f"  Lỗi đổi partition (servers={servers}): {e}")
            continue
    return False


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

    spark_ok = False
    for url in [SPARK_MASTER_URL, "http://localhost:8080/json/"]:
        try:
            r = requests.get(url, timeout=2)
            active = len(r.json().get('activeapps', []))
            if active == 0:
                err("Không có Spark app đang chạy")
                sys.exit(1)
            ok(f"Spark app active ({active})")
            spark_ok = True
            break
        except Exception:
            continue
    if not spark_ok:
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
        'levels': [100, 500, 1000, 2000, 3000, 5000],
        'duration': 30,
        'sustained_duration': 60,
        'description': 'tăng đến bottleneck',
    },
    'bottleneck_hunting': {
        'levels': [100, 500, 1000, 2000, 3000, 5000],
        'duration': 30,
        'sustained_duration': 60,
        'max_drain_s': 300,
        'description': 'bottleneck hunting — drain timeout 5min, đầy đủ lag/drain metrics',
    },
    'partition': {
        'levels': [100, 500, 1000, 2000],
        'duration': 30,
        'sustained_duration': 60,
        'partitions': 3,
        'description': 'test với 3 partitions',
    },
    'realistic': {
        'levels': [100, 200, 500],
        'duration': 30,
        'sustained_duration': 60,
        'description': 'mix INSERT(60%)/UPDATE(30%)/DELETE(10%) — phản ánh traffic production thực tế',
    },
    'sustained10m': {
        'levels': [2000],
        'duration': 30,
        'sustained_duration': 600,
        'max_drain_s': 300,
        'description': 'Phase 4 — 1 level 2000 rec/s + sustained 10 phút (600s)',
    },
}


# ══════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="CDC Benchmark v4 — E2E TPS thật")
    parser.add_argument('mode', nargs='?', default='full',
                        choices=list(MODES.keys()),
                        help='quick | full | stress | partition | realistic')
    parser.add_argument('--partitions', type=int, default=None,
                        help='Target Kafka partition count (dùng cho scale test)')
    args = parser.parse_args()

    cfg = MODES[args.mode]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / f"run_{timestamp}.json"

    print(f"{C.BOLD}{C.B}")
    print("╔═══════════════════════════════════════════════════════╗")
    print("║    CDC Pipeline — Benchmark v4 (E2E records/s thật)  ║")
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

    # --partitions override (dùng cho scale test, hoạt động ở mọi mode)
    if args.partitions and args.partitions > current_partitions:
        step(f"Đổi Kafka partitions → {args.partitions}")
        change_partitions(args.partitions)
        current_partitions = get_current_partitions()
        ok(f"Partitions hiện tại: {current_partitions}")
    elif args.partitions and args.partitions <= current_partitions:
        info(f"Đã có {current_partitions} partitions (≥ {args.partitions}), không cần đổi")

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
    spark_engine = detect_spark_engine()
    spark_worker_count = get_spark_worker_count()
    spark_info = {
        "executor_cores": int(sm.get('spark_cores', 0)),
        "executor_memory_mb": int(sm.get('spark_mem', 0)),
        "kafka_partitions": current_partitions,
        "engine": spark_engine,
        "worker_count": spark_worker_count,
    }
    info(f"Spark cores: {spark_info['executor_cores']}, Memory: {spark_info['executor_memory_mb']}MB")
    info(f"Spark workers: {spark_worker_count}, Engine: {spark_engine.upper()}")

    # Warmup
    step("Khởi động nóng (10s, 5 records/s)")
    cleanup()
    inject_load(5, 10)
    time.sleep(10)
    cleanup()
    ok("Xong")

    # ── E2E Latency per-record ────────────────────────────
    step("Đo E2E latency per-record (P50/P95/P99) — 20 probes")
    latency_result = measure_e2e_latency(n_probes=20)
    if latency_result:
        print(f"  {C.DIM}P50:{C.X}  {latency_result['p50_ms']}ms")
        print(f"  {C.DIM}P95:{C.X}  {latency_result['p95_ms']}ms")
        print(f"  {C.DIM}P99:{C.X}  {latency_result['p99_ms']}ms")
        print(f"  {C.DIM}Avg:{C.X}  {latency_result['avg_ms']}ms  |  Min: {latency_result['min_ms']}ms  Max: {latency_result['max_ms']}ms")
        print(f"  {C.DIM}Note:{C.X} Bao gồm 1 trigger cycle Spark (5s). P50 thường 1–6s.")
    else:
        warn("  Bỏ qua latency measurement — xem lỗi ở trên")
        latency_result = None

    # ── Ramp-up test ─────────────────────────────────────
    step("Đo E2E records/s thật — tăng dần tải")

    ramp_results = []
    max_e2e_tps = 0
    bottleneck = None

    for target in cfg['levels']:
        print()
        info(f"Mức: {C.BOLD}{target} records/s inject{C.X} × {cfg['duration']}s")

        cleanup()
        time.sleep(3)

        if args.mode == 'realistic':
            result = run_e2e_realistic_test(target, cfg['duration'])
            print(f"  {C.DIM}Inject rate:{C.X}      {result['inject_rate']} events/s")
            print(f"  {C.DIM}  INSERT:{C.X}          {result['breakdown']['insert']}")
            print(f"  {C.DIM}  UPDATE:{C.X}          {result['breakdown']['update']}")
            print(f"  {C.DIM}  DELETE:{C.X}          {result['breakdown']['delete']}")
            print(f"  {C.DIM}E2E records/s:{C.X}    {C.BOLD}{result['e2e_tps']}{C.X}  ← Kafka delta/tổng thời gian")
            print(f"  {C.DIM}Kafka delta:{C.X}       {result['kafka_delta']} events")
        else:
            result = run_e2e_test(target, cfg['duration'],
                                  max_drain_s=cfg.get('max_drain_s', 120))
            print(f"  {C.DIM}Inject rate:{C.X}      {result['inject_tps']} records/s")
            print(f"  {C.DIM}E2E records/s:{C.X}    {C.BOLD}{result['e2e_tps']}{C.X}  ← con số thật")
            print(f"  {C.DIM}MySQL delta:{C.X}       {result['mysql_delta']}")
            print(f"  {C.DIM}Mongo delta:{C.X}       {result['mongo_delta']}")

        print(f"  {C.DIM}Lag cuối inject:{C.X}   {result['lag_at_inject_end']} rec (backlog tích lũy)")
        print(f"  {C.DIM}Peak lag:{C.X}          {result['peak_lag']} rec")
        print(f"  {C.DIM}Lag còn lại:{C.X}       {result['lag_remaining']} rec")
        print(f"  {C.DIM}Drain rate:{C.X}        {result['drain_rate_rps']} rec/s (tốc độ pipeline catch-up)")
        print(f"  {C.DIM}Thời gian inject:{C.X}  {result['inject_elapsed_s']}s")
        print(f"  {C.DIM}Thời gian drain:{C.X}   {result['drain_elapsed_s']}s")
        print(f"  {C.DIM}TỔNG thời gian:{C.X}    {result['total_elapsed_s']}s")
        print(f"  {C.DIM}Spark batch:{C.X}        avg {result['spark_batch_avg_ms']}ms, p50 {result['spark_batch_p50_ms']}ms, p95 {result['spark_batch_p95_ms']}ms, p99 {result['spark_batch_p99_ms']}ms")
        print(f"  {C.DIM}Kafka rate:{C.X}         {result['kafka_rate_avg']} events/s")

        ramp_results.append(result)

        # Bottleneck?
        if result['lag_remaining'] > 0 or not result['synced']:
            warn(f"BOTTLENECK tại {target} records/s inject → E2E thật chỉ {result['e2e_tps']} records/s")
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
            max_e2e_tps = max(max_e2e_tps, result['e2e_tps'])
            break
        else:
            ok(f"E2E records/s: {result['e2e_tps']} — pipeline kịp xử lý")
            max_e2e_tps = max(max_e2e_tps, result['e2e_tps'])

    # ── Sustained test ────────────────────────────────────
    sustained_target = max(50, int(max_e2e_tps * 0.8))
    step(f"Chạy ổn định {sustained_target} records/s × {cfg['sustained_duration']}s")

    cleanup()
    time.sleep(3)

    sustained_result = run_e2e_test(sustained_target, cfg['sustained_duration'],
                                    max_drain_s=cfg.get('max_drain_s', 120))

    print(f"  {C.DIM}E2E records/s:{C.X}    {C.BOLD}{sustained_result['e2e_tps']}{C.X}")
    print(f"  {C.DIM}Records:{C.X}           {sustained_result['mongo_delta']}")
    print(f"  {C.DIM}Lag:{C.X}               {sustained_result['lag_remaining']}")
    print(f"  {C.DIM}Tổng thời gian:{C.X}    {sustained_result['total_elapsed_s']}s")
    print(f"  {C.DIM}Spark p50/p95/p99:{C.X}  {sustained_result['spark_batch_p50_ms']}/{sustained_result['spark_batch_p95_ms']}/{sustained_result['spark_batch_p99_ms']}ms")

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

        "e2e_latency": latency_result,

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

    import shutil
    latest_file = RESULTS_DIR / "latest_benchmark.json"
    shutil.copy(result_file, latest_file)
    info(f"Đã copy sang: {latest_file}")

    # ── Ghi history.jsonl (1 dòng / run, dùng compare_runs.py để so sánh) ──
    history_file = RESULTS_DIR / "history.jsonl"
    hw_short = f"{hw.get('cpu_model','?')[:30]}, {hw.get('cpu_cores','?')}c, {hw.get('ram_gb','?')}GB"
    history_entry = {
        "ts":            report["timestamp"],
        "run_id":        report["run_id"],
        "mode":          args.mode,
        "engine":        spark_info.get("engine", "unknown"),
        "hw":            hw_short,
        "max_e2e_tps":   max_e2e_tps,
        "sus_tps":       sustained_result.get("e2e_tps"),
        "spark_p50_ms":  sustained_result.get("spark_batch_p50_ms"),
        "spark_p95_ms":  sustained_result.get("spark_batch_p95_ms"),
        "spark_p99_ms":  sustained_result.get("spark_batch_p99_ms"),
        "kafka_rate":    sustained_result.get("kafka_rate_avg"),
        "peak_lag":      sustained_result.get("peak_lag"),
        "drain_rate":    sustained_result.get("drain_rate_rps"),
        "partitions":    current_partitions,
        "spark_workers": spark_worker_count,
        "bottleneck":    bottleneck["stage"] if bottleneck else None,
        "e2e_p50_ms":    latency_result["p50_ms"] if latency_result else None,
        "e2e_p95_ms":    latency_result["p95_ms"] if latency_result else None,
        "e2e_p99_ms":    latency_result["p99_ms"] if latency_result else None,
        "result_file":   str(result_file.name),
    }
    with open(history_file, "a", encoding="utf-8") as hf:
        hf.write(json.dumps(history_entry, ensure_ascii=False) + "\n")
    info(f"History: {history_file}")

    # ── Summary ───────────────────────────────────────────
    step("KẾT QUẢ")

    print(f"\n  🎯 Max E2E records/s:   {C.BOLD}{max_e2e_tps}{C.X}")

    if bottleneck:
        print(f"  🚧 Bottleneck tại:      {bottleneck['at_tps']} records/s (inject)")
        print(f"  🎯 E2E thật khi nghẽn:  {bottleneck['e2e_tps']} records/s")
        print(f"  📍 Tầng nghẽn:          {bottleneck['stage']}")
    else:
        print(f"  ✅ Không bottleneck: pipeline kịp xử lý tất cả mức test")

    sr = report['sustained']
    print(f"\n  📊 Chạy ổn định ({sustained_target} records/s × {cfg['sustained_duration']}s):")
    print(f"      E2E records/s:    {sr['e2e_tps']}")
    print(f"      Records đến Mongo: {sr['mongo_delta']}")
    print(f"      Lag còn lại:      {sr['lag_remaining']}")
    print(f"      Spark p50/p95/p99: {sr['spark_batch_p50_ms']}/{sr['spark_batch_p95_ms']}/{sr['spark_batch_p99_ms']}ms")
    print(f"      Kafka partitions: {current_partitions}")

    if latency_result:
        print(f"\n  ⏱  E2E Latency per-record (n={latency_result['n_measured']}/{latency_result['n_probes']}):")
        print(f"      P50: {latency_result['p50_ms']}ms")
        print(f"      P95: {latency_result['p95_ms']}ms")
        print(f"      P99: {latency_result['p99_ms']}ms")
        print(f"      Avg: {latency_result['avg_ms']}ms  (Min {latency_result['min_ms']}ms / Max {latency_result['max_ms']}ms)")

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
