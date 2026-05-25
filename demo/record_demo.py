#!/usr/bin/env python3
"""
CDC Demo Recorder — ghi lại toàn bộ metrics trong quá trình demo.
Chạy song song với demo_server.py trên cửa sổ terminal riêng.

Usage:
    python3 demo/record_demo.py              # interval mặc định 3s
    python3 demo/record_demo.py --interval 5 # poll mỗi 5s

Output:
    demo/recordings/demo_YYYY-MM-DD_HH-MM-SS.jsonl
    demo/recordings/demo_YYYY-MM-DD_HH-MM-SS_summary.txt  (khi Ctrl+C)
"""

import os, sys, json, time, signal, math
import urllib.request, urllib.parse
from pathlib import Path
from datetime import datetime

# ── Load .env (cùng cách demo_server.py) ───────────────────────────────
_env = Path(__file__).parent / ".env"
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

PROM_URL   = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090")
REDIS_HOST = os.getenv("REDIS_HOST",     "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MONGO_URI  = os.getenv("MONGO_URI",      "mongodb://127.0.0.1:27017")
MYSQL_HOST = os.getenv("MYSQL_HOST",     "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER",     "root")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DB   = os.getenv("MYSQL_DB",       "inventory")

INTERVAL = 3
if "--interval" in sys.argv:
    try:
        INTERVAL = int(sys.argv[sys.argv.index("--interval") + 1])
    except (IndexError, ValueError):
        pass

# ── Output ──────────────────────────────────────────────────────────────
_rec_dir = Path(__file__).parent / "recordings"
_rec_dir.mkdir(exist_ok=True)
_ts_str  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTFILE  = _rec_dir / f"demo_{_ts_str}.jsonl"
SUMFILE  = _rec_dir / f"demo_{_ts_str}_summary.txt"

# ── Optional dependencies ────────────────────────────────────────────────
try:
    import redis as _redis_lib
    _redis = _redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT,
                              decode_responses=True, socket_timeout=2)
    _redis.ping()
except Exception:
    _redis = None

try:
    import pymongo
    _mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    _mongo_client.admin.command("ping")
    _mongo = _mongo_client["inventory"]
except Exception:
    _mongo = None

try:
    import pymysql
except ImportError:
    pymysql = None

# ── Data collectors ──────────────────────────────────────────────────────
def _prom(query: str) -> float:
    try:
        url = f"{PROM_URL}/api/v1/query?query={urllib.parse.quote(query)}"
        with urllib.request.urlopen(url, timeout=3) as r:
            data = json.loads(r.read())
        res = data["data"]["result"]
        return float(res[0]["value"][1]) if res else 0.0
    except Exception:
        return 0.0

def _redis_get(key: str) -> float:
    try:
        if _redis:
            v = _redis.get(key)
            return float(v) if v else 0.0
    except Exception:
        pass
    return 0.0

def _mongo_count(col: str) -> int:
    try:
        if _mongo:
            return _mongo[col].count_documents({})
    except Exception:
        pass
    return -1

def _mysql_count() -> int:
    try:
        if pymysql:
            conn = pymysql.connect(
                host=MYSQL_HOST, port=MYSQL_PORT,
                user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
                autocommit=True, connect_timeout=2,
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customers")
            n = cur.fetchone()[0]
            conn.close()
            return n
    except Exception:
        pass
    return -1

def _docker_broker_count() -> int:
    """Đếm số Kafka broker container đang running."""
    try:
        import subprocess
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=cdc-kafka", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3,
        )
        return len([l for l in r.stdout.splitlines() if l.strip()])
    except Exception:
        return 1

# ── Collect one sample ───────────────────────────────────────────────────
def collect() -> dict:
    return {
        "ts":            int(time.time()),
        "time":          datetime.now().strftime("%H:%M:%S"),
        # Pipeline counts
        "mysql_cust":    _mysql_count(),
        "mongo_cust":    _mongo_count("customers"),
        "mongo_orders":  _mongo_count("orders"),
        "redis_cust":    int(_redis_get("customers:total")),
        "redis_orders":  int(_redis_get("orders:total")),
        # Throughput
        "insert_rate":   round(_prom("cdc_mysql_insert_rate"), 1),
        "mongo_rate":    round(_prom("cdc_mongo_write_rate"),  1),
        "kafka_rate":    round(_prom("cdc_kafka_rate_total"),  1),
        # Kafka lag
        "kafka_lag":     round(_prom("cdc_kafka_consumer_lag"), 0),
        # Spark
        "spark_batch_ms": int(_redis_get("spark:batch_duration_ms")),
        # Infrastructure
        "pipeline_up":   int(_prom("cdc_pipeline_up")),
        "kafka_brokers": _docker_broker_count(),
        # Benchmark (nếu đang chạy)
        "bench_e2e_rate": round(_prom("cdc_benchmark_throughput_e2e"), 1),
    }

# ── Display ──────────────────────────────────────────────────────────────
HDR = (
    f"{'Time':8} {'MySQL':>7} {'Mongo':>7} {'Lag':>7} "
    f"{'Rate/s':>7} {'SprkMs':>7} {'Brk':>4} {'Up':>3}"
)
SEP = "─" * len(HDR)

def _fmt_row(s: dict) -> str:
    lag_str = f"{s['kafka_lag']:.0f}"
    if s["kafka_lag"] > 500:
        lag_str = f"\033[31m{lag_str}\033[0m"   # red
    elif s["kafka_lag"] > 100:
        lag_str = f"\033[33m{lag_str}\033[0m"   # yellow

    up = "\033[32mOK\033[0m" if s["pipeline_up"] else "\033[31mDN\033[0m"

    return (
        f"{s['time']:8} {s['mysql_cust']:>7} {s['mongo_cust']:>7} "
        f"{lag_str:>7} {s['insert_rate']:>7.1f} "
        f"{s['spark_batch_ms']:>7} {s['kafka_brokers']:>4} {up:>3}"
    )

# ── Summary ──────────────────────────────────────────────────────────────
def summarize(samples: list, duration_s: int) -> str:
    if not samples:
        return "No samples recorded."

    peak_rate    = max(s["insert_rate"]   for s in samples)
    peak_lag     = max(s["kafka_lag"]     for s in samples)
    peak_mongo   = max(s["mongo_cust"]    for s in samples)
    peak_mysql   = max(s["mysql_cust"]    for s in samples)
    avg_batch    = sum(s["spark_batch_ms"] for s in samples) / len(samples)
    max_batch    = max(s["spark_batch_ms"] for s in samples)
    peak_brokers = max(s["kafka_brokers"] for s in samples)

    non_zero_rates = [s["insert_rate"] for s in samples if s["insert_rate"] > 0]
    avg_rate = sum(non_zero_rates) / len(non_zero_rates) if non_zero_rates else 0

    lines = [
        "=" * 60,
        "CDC DEMO RECORDING SUMMARY",
        "=" * 60,
        f"Duration       : {duration_s}s  ({len(samples)} samples @ {INTERVAL}s interval)",
        f"Output file    : {OUTFILE}",
        "",
        "── Throughput ──────────────────────────────────────",
        f"Peak insert rate : {peak_rate:.1f} rec/s",
        f"Avg insert rate  : {avg_rate:.1f} rec/s",
        "",
        "── Data counts (peak) ──────────────────────────────",
        f"MySQL customers  : {peak_mysql:,}",
        f"MongoDB customers: {peak_mongo:,}",
        f"MongoDB orders   : {max(s['mongo_orders'] for s in samples):,}",
        "",
        "── Pipeline health ─────────────────────────────────",
        f"Peak Kafka lag   : {peak_lag:.0f} records",
        f"Avg Spark batch  : {avg_batch:.0f} ms",
        f"Max Spark batch  : {max_batch} ms",
        f"Max Kafka brokers: {peak_brokers}",
        "=" * 60,
    ]

    # Thêm e2e benchmark nếu có
    bench_samples = [s for s in samples if s.get("bench_e2e_rate", 0) > 0]
    if bench_samples:
        peak_bench = max(s["bench_e2e_rate"] for s in bench_samples)
        lines.insert(-1, f"Peak E2E rate    : {peak_bench:.1f} rec/s (benchmark)")

    return "\n".join(lines)

# ── Main loop ────────────────────────────────────────────────────────────
samples    = []
start_time = time.time()
row_count  = 0

def on_exit(sig, frame):
    duration = int(time.time() - start_time)
    summary  = summarize(samples, duration)

    print(f"\n{SEP}")
    print(summary)

    SUMFILE.write_text(summary, encoding="utf-8")
    print(f"\nSummary saved: {SUMFILE}")
    sys.exit(0)

signal.signal(signal.SIGINT,  on_exit)
signal.signal(signal.SIGTERM, on_exit)

print(f"CDC Demo Recorder  |  interval={INTERVAL}s  |  Ctrl+C to stop + summary")
print(f"JSONL → {OUTFILE}")
print(SEP)
print(HDR)
print(SEP)

while True:
    s = collect()
    samples.append(s)

    # Ghi file
    with open(OUTFILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(s) + "\n")

    # In row, reprint header mỗi 20 dòng
    if row_count % 20 == 0 and row_count > 0:
        print(SEP)
        print(HDR)
        print(SEP)
    print(_fmt_row(s))
    row_count += 1

    time.sleep(INTERVAL)
