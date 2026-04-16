#!/usr/bin/env python3
"""
scale_and_measure.py
====================
Tăng Kafka partition từ 1 → 3, đo throughput trước/sau, sinh JSON kết quả.

Cách chạy:
    python3 scale_and_measure.py

Lưu ý:
  - Kafka CHỈ tăng được partition (không giảm được)
  - Spark job PHẢI restart sau khi tăng partition để nhận partition mới
  - Script sẽ hỏi confirm trước khi restart Spark
"""

import time
import subprocess
import json
import pymysql
import pymongo
import statistics
from datetime import datetime

MYSQL_CFG = dict(host="127.0.0.1", port=3306, user="root",
                 password="root", database="inventory", autocommit=True)
MONGO_URI  = "mongodb://127.0.0.1:27017"
BASE_ID    = 80_000
TOPICS     = [
    "inventory.inventory.orders",
    "inventory.inventory.customers",
]
SPARK_JAR  = "/opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar"
CHECKPOINT = "/tmp/spark-checkpoint/cdc-pipeline"
N_RECORDS  = 100   # records mỗi lần đo throughput
WAIT_SYNC  = 45    # giây chờ MongoDB sync

# ────────────────────────────────────────────────────────────────────

def shell(cmd, capture=True):
    r = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    return r.stdout.strip(), r.returncode

def get_partitions(topic):
    out, _ = shell(
        f"docker exec cdc-kafka kafka-topics "
        f"--bootstrap-server localhost:29092 "
        f"--describe --topic {topic} 2>/dev/null"
    )
    for line in out.splitlines():
        if "PartitionCount" in line:
            return int(line.split("PartitionCount:")[1].split()[0])
    return -1

def set_partitions(topic, n):
    _, rc = shell(
        f"docker exec cdc-kafka kafka-topics "
        f"--bootstrap-server localhost:29092 "
        f"--alter --topic {topic} --partitions {n}"
    )
    return rc == 0

def get_mongo_col():
    return pymongo.MongoClient(MONGO_URI)["inventory"]["customers"]

def cleanup_test(conn, mongo, start_id):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM customers WHERE id >= {start_id}")
    mongo.delete_many({"_id": {"$gte": start_id}})

def measure_throughput(conn, mongo, start_id, n, label):
    """Insert n records → đo E2E throughput."""
    print(f"  Inserting {n} records [{label}]...")
    records = [
        (start_id + i, f"scale_{start_id+i}",
         f"s{start_id+i}@test.com", "0911000000")
        for i in range(n)
    ]
    t0 = time.time()
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO customers (id, name, email, phone) VALUES (%s,%s,%s,%s)",
            records
        )
    insert_time = time.time() - t0

    deadline = time.time() + WAIT_SYNC
    while time.time() < deadline:
        synced = mongo.count_documents(
            {"_id": {"$gte": start_id, "$lt": start_id + n}}
        )
        if synced >= n:
            break
        time.sleep(1)

    total_time = time.time() - t0
    synced = mongo.count_documents(
        {"_id": {"$gte": start_id, "$lt": start_id + n}}
    )
    e2e_tps = round(synced / total_time, 2) if total_time > 0 else 0
    cleanup_test(conn, mongo, start_id)

    result = {
        "label": label,
        "records_sent": n,
        "records_synced": synced,
        "insert_time_s": round(insert_time, 3),
        "total_time_s": round(total_time, 2),
        "e2e_tps": e2e_tps,
        "sync_pct": round(synced / n * 100, 1),
    }
    print(f"  → E2E: {e2e_tps} rec/s | Synced: {synced}/{n} | Time: {round(total_time,1)}s")
    return result

def measure_latency(conn, mongo, test_id, timeout=20):
    """Insert 1 record → đo latency."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (id, name, email, phone) VALUES (%s,%s,%s,%s)",
            (test_id, f"lat_{test_id}", f"l{test_id}@test.com", "0900000003")
        )
    t0 = time.time()
    while time.time() - t0 < timeout:
        if mongo.count_documents({"_id": test_id}) > 0:
            lat = round(time.time() - t0, 3)
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM customers WHERE id={test_id}")
            mongo.delete_one({"_id": test_id})
            return lat
        time.sleep(0.2)
    return None

def percentile(data, p):
    if not data:
        return None
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return round(s[f] + (k - f) * (s[c] - s[f]), 3)

def restart_spark_job():
    print("\n  Restarting Spark job để nhận partition mới...")
    # Kill process cũ
    shell("docker exec cdc-spark-master bash -c "
          "\"pkill -f CdcRedisConsumer 2>/dev/null; sleep 2\"")
    time.sleep(5)
    # Submit lại
    cmd = (
        f"docker exec -d cdc-spark-master bash -c "
        f"'/opt/spark/bin/spark-submit "
        f"--class CdcRedisConsumer "
        f"--master spark://cdc-spark-master:7077 "
        f"--conf spark.executor.memory=1g "
        f"--conf spark.driver.memory=512m "
        f"{SPARK_JAR} > /tmp/spark_metrics.log 2>&1'"
    )
    _, rc = shell(cmd)
    if rc == 0:
        print("  ✅ Spark job restarted")
        print("  Chờ 25s để job ổn định...")
        time.sleep(25)
        return True
    else:
        print("  ❌ Restart failed")
        return False

# ── Main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  CDC PIPELINE — KAFKA PARTITION SCALING TEST")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn  = pymysql.connect(**MYSQL_CFG)
    mongo = get_mongo_col()
    all_results = {"timestamp": datetime.now().isoformat()}
    cur_id = BASE_ID

    # ── Đo BASELINE (1 partition) ─────────────────────────────────
    print("\n[ PHASE 1: BASELINE — 1 partition ]")
    for topic in TOPICS:
        p = get_partitions(topic)
        print(f"  {topic}: {p} partition(s)")

    # Latency baseline (5 lần)
    print("\n  Đo latency baseline (5 lần)...")
    lat_baseline = []
    for i in range(5):
        lat = measure_latency(conn, mongo, cur_id)
        cur_id += 1
        if lat:
            lat_baseline.append(lat)
            print(f"    Lần {i+1}: {lat}s")
        time.sleep(6)

    # Throughput baseline
    tp_baseline = measure_throughput(conn, mongo, cur_id, N_RECORDS, "1-partition baseline")
    cur_id += N_RECORDS

    all_results["baseline"] = {
        "partitions": 1,
        "latency_p50": percentile(lat_baseline, 50),
        "latency_p95": percentile(lat_baseline, 95),
        "latency_p99": percentile(lat_baseline, 99),
        "latency_avg": round(statistics.mean(lat_baseline), 3) if lat_baseline else None,
        "throughput": tp_baseline,
    }

    print(f"\n  Baseline latency: p50={percentile(lat_baseline,50)}s "
          f"p95={percentile(lat_baseline,95)}s "
          f"p99={percentile(lat_baseline,99)}s")

    # ── Tăng partition ────────────────────────────────────────────
    print("\n[ PHASE 2: SCALE UP — tăng lên 3 partitions ]")
    confirm = input("  Tiếp tục tăng Kafka partition 1→3 và restart Spark? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("  Bỏ qua scale test. Lưu kết quả baseline...")
        _save(all_results)
        return

    success_count = 0
    for topic in TOPICS:
        ok = set_partitions(topic, 3)
        status = "✅" if ok else "❌"
        print(f"  {status} {topic} → 3 partitions")
        if ok:
            success_count += 1

    if success_count == 0:
        print("  ❌ Không tăng được partition nào.")
        _save(all_results)
        return

    # Verify
    time.sleep(3)
    for topic in TOPICS:
        p = get_partitions(topic)
        print(f"  Verify: {topic} = {p} partition(s)")

    # Restart Spark
    restarted = restart_spark_job()
    if not restarted:
        print("  ⚠️  Spark không restart được. Kết quả scale có thể không chính xác.")

    # ── Đo SAU KHI SCALE (3 partition) ───────────────────────────
    print("\n[ PHASE 3: AFTER SCALE — 3 partitions ]")

    # Latency sau scale (5 lần)
    print("\n  Đo latency sau scale (5 lần)...")
    lat_scaled = []
    for i in range(5):
        lat = measure_latency(conn, mongo, cur_id)
        cur_id += 1
        if lat:
            lat_scaled.append(lat)
            print(f"    Lần {i+1}: {lat}s")
        time.sleep(6)

    # Throughput sau scale
    tp_scaled = measure_throughput(conn, mongo, cur_id, N_RECORDS, "3-partition scaled")
    cur_id += N_RECORDS

    all_results["scaled"] = {
        "partitions": 3,
        "latency_p50": percentile(lat_scaled, 50),
        "latency_p95": percentile(lat_scaled, 95),
        "latency_p99": percentile(lat_scaled, 99),
        "latency_avg": round(statistics.mean(lat_scaled), 3) if lat_scaled else None,
        "throughput": tp_scaled,
    }

    # ── So sánh ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  KẾT QUẢ SO SÁNH")
    print("=" * 60)

    b = all_results["baseline"]
    s = all_results["scaled"]

    tp_improvement = 0
    if b["throughput"]["e2e_tps"] > 0:
        tp_improvement = round(
            (s["throughput"]["e2e_tps"] - b["throughput"]["e2e_tps"])
            / b["throughput"]["e2e_tps"] * 100, 1
        )

    comparison = {
        "baseline_partitions": 1,
        "scaled_partitions": 3,
        "baseline_tps": b["throughput"]["e2e_tps"],
        "scaled_tps": s["throughput"]["e2e_tps"],
        "throughput_improvement_pct": tp_improvement,
        "baseline_p95_s": b["latency_p95"],
        "scaled_p95_s": s["latency_p95"],
    }
    all_results["comparison"] = comparison

    print(f"\n  Metric              1 Partition    3 Partitions   Change")
    print(f"  " + "-" * 55)
    print(f"  Throughput E2E     {b['throughput']['e2e_tps']:>10.1f}/s   "
          f"{s['throughput']['e2e_tps']:>10.1f}/s   "
          f"+{tp_improvement}%")
    print(f"  Latency p50        {str(b['latency_p50'])+'s':>12}   "
          f"{str(s['latency_p50'])+'s':>12}")
    print(f"  Latency p95        {str(b['latency_p95'])+'s':>12}   "
          f"{str(s['latency_p95'])+'s':>12}")
    print(f"  Latency p99        {str(b['latency_p99'])+'s':>12}   "
          f"{str(s['latency_p99'])+'s':>12}")

    _save(all_results)
    conn.close()

def _save(results):
    with open("scaling_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print("\n✅ Kết quả đã lưu vào: scaling_results.json")
    print("   (dùng file này cho Grafana dashboard)")

if __name__ == "__main__":
    main()