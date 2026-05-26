#!/usr/bin/env python3
"""
CDC Data Pipeline — Live Demo Server
Run  : python demo_server.py   (sau khi: pip install -r requirements.txt)
Open : http://localhost:8888
"""

import os, sys, json, time, threading, random, subprocess, statistics
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Load .env từ cùng thư mục (nếu có) — không cần python-dotenv
_env = Path(__file__).parent / ".env"
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

from flask import Flask, jsonify, send_from_directory, request
import pymysql
import pymongo
import redis as redis_lib
import urllib.request

# ── Config (override via .env hoặc env vars) ────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DB   = os.getenv("MYSQL_DB", "inventory")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MONGO_URI  = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")

PROM_URL   = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090")
DEMO_PORT  = int(os.getenv("DEMO_PORT", "8888"))

DEMO_DIR = Path(__file__).parent

# ── Load injection state ─────────────────────────────────────────────
_state = {
    "running":  False,
    "rate":     100,
    "injected": 0,
    "errors":   0,
    "start_ts": None,
    "thread":   None,
}
_lock = threading.Lock()

# Base offsets recorded at clear time — dashboard shows relative values
_kafka_base = {"customers": 0.0, "orders": 0.0}

_NAMES = [
    "Nguyễn Văn An", "Trần Thị Bích", "Lê Hoàng Cường", "Phạm Thị Dung",
    "Hoàng Văn Em",  "Đặng Thị Fương","Bùi Văn Giang",  "Đỗ Thị Hoa",
    "Ngô Văn Hùng",  "Vũ Thị Lan",    "Dương Văn Minh", "Phan Thị Ngọc",
    "Trương Văn Phúc","Đinh Thị Quỳnh","Lý Văn Sơn",    "Tô Thị Tâm",
    "Mai Văn Tuấn",  "Lâm Thị Uyên",  "Cao Văn Vinh",  "Hà Thị Xuân",
]
_DOMAINS  = ["gmail.com", "yahoo.com", "outlook.com", "company.vn", "mail.vn", "edu.vn"]
_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]

def _rand_email(name: str) -> str:
    slug = "".join(c for c in name.split()[-1].lower()
                   if c.isascii() and c.isalpha()) or "user"
    return f"{slug}{random.randint(10, 9999)}@{random.choice(_DOMAINS)}"

def _rand_phone() -> str:
    return f"0{random.randint(900_000_000, 999_999_999)}"

def _load_worker(rate: int):
    """Background thread: INSERT vào MySQL ở tốc độ target."""
    # Mục tiêu: ~10 batches/s ở mọi rate (ổn định hơn sleep nhỏ)
    # batch_size = rate/10, cap ở 200 rows (MySQL executemany limit thực tế)
    batch_size = max(1, min(rate // 10, 200))
    interval   = batch_size / rate  # giây giữa mỗi batch

    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=False, charset="utf8mb4",
        )
    except Exception as e:
        print(f"[LoadWorker] MySQL connect failed: {e}")
        with _lock:
            _state["running"] = False
        return

    print(f"[LoadWorker] Started — {rate} rec/s, batch={batch_size}, interval={interval:.3f}s")

    while True:
        with _lock:
            if not _state["running"]:
                break
        try:
            cur = conn.cursor()

            # Insert customers
            _names_batch = [random.choice(_NAMES) for _ in range(batch_size)]
            cust_rows = [
                (n, _rand_email(n), _rand_phone()) for n in _names_batch
            ]
            cur.executemany(
                "INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
                cust_rows,
            )
            first_id = cur.lastrowid  # first auto_increment ID of this batch

            # Insert orders — ~60% customers nhận 1-2 orders
            order_rows = []
            for i in range(batch_size):
                if random.random() < 0.6:
                    cust_id = first_id + i
                    n_orders = random.randint(1, 2)
                    for _ in range(n_orders):
                        amount = round(random.uniform(50_000, 3_000_000), 2)
                        status = random.choice(_STATUSES)
                        order_rows.append((cust_id, amount, status))
            if order_rows:
                cur.executemany(
                    "INSERT INTO orders (customer_id, total_amount, status) VALUES (%s, %s, %s)",
                    order_rows,
                )

            conn.commit()
            with _lock:
                _state["injected"] += batch_size
        except Exception as e:
            print(f"[LoadWorker] Insert error: {e}")
            with _lock:
                _state["errors"] += 1

        time.sleep(interval)

    conn.close()
    print("[LoadWorker] Stopped.")


# ── Fault tolerance state ────────────────────────────────────────────
# ── Capacity stress test — configurable via .env ─────────────────────
# VM example: STRESS_RATES=500,1000,2000,5000,10000  STRESS_DURATION=60
STRESS_RATES    = [int(x) for x in os.getenv("STRESS_RATES",    "50,100,150,200,250,300,400").split(",")]
STRESS_DURATION = int(os.getenv("STRESS_DURATION", "40"))   # seconds per level
# lag > rate * SAT_RATIO → saturating (floor 50 to absorb jitter)
STRESS_SAT_RATIO = float(os.getenv("STRESS_SAT_RATIO", "0.5"))

_stress_state: dict = {
    "running":         False,
    "phase":           "idle",   # idle | running | done
    "current_level":   -1,
    "current_rate":    0,
    "levels":          STRESS_RATES,
    "duration_s":      STRESS_DURATION,
    "sat_ratio":       STRESS_SAT_RATIO,
    "results":         [],
    "saturation_rate": None,
}
_stress_lock = threading.Lock()


def _get_prometheus_metric(name: str):
    try:
        url = f"{PROM_URL}/api/v1/query?query={name}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            data = json.loads(resp.read())
        rs = data.get("data", {}).get("result", [])
        return float(rs[0]["value"][1]) if rs else None
    except Exception:
        return None


def _stress_worker():
    for level_idx, rate in enumerate(STRESS_RATES):
        with _stress_lock:
            if not _stress_state["running"]:
                break
            _stress_state["current_level"] = level_idx
            _stress_state["current_rate"]  = rate

        # Stop previous injection then wait for thread to exit
        with _lock:
            _state["running"] = False
        time.sleep(2)

        lag_before = _get_prometheus_metric("cdc_lag_total") or 0.0

        # Start injection at this rate
        with _lock:
            _state.update(running=True, rate=rate, injected=0, errors=0, start_ts=time.time())
            t = threading.Thread(target=_load_worker, args=(rate,), daemon=True)
            _state["thread"] = t
        t.start()

        # Warm-up 10s, then sample every 5s for remaining duration
        time.sleep(10)
        batch_samples, consumer_lag_samples, inject_samples = [], [], []
        mongo_rate_samples, kafka_rate_samples = [], []
        sample_count = (STRESS_DURATION - 10) // 5
        for _ in range(sample_count):
            with _stress_lock:
                if not _stress_state["running"]:
                    break
            # cdc_kafka_consumer_lag: delta(kafka_events) - delta(mongo_writes) per 5s window
            # more accurate than cdc_lag_total for saturation detection
            c_lag    = _get_prometheus_metric("cdc_kafka_consumer_lag") or 0.0
            batch_ms = _get_prometheus_metric("cdc_spark_batch_duration_ms") or 0.0
            inj_r    = _get_prometheus_metric("cdc_mysql_insert_rate") or 0.0
            mongo_r  = _get_prometheus_metric("cdc_mongo_write_rate") or 0.0
            kafka_r  = _get_prometheus_metric("cdc_kafka_rate_total") or 0.0
            consumer_lag_samples.append(c_lag)
            if batch_ms > 0: batch_samples.append(batch_ms)
            if inj_r   > 0: inject_samples.append(inj_r)
            if mongo_r > 0: mongo_rate_samples.append(mongo_r)
            if kafka_r > 0: kafka_rate_samples.append(kafka_r)
            time.sleep(5)

        with _stress_lock:
            if not _stress_state["running"]:
                break

        avg_consumer_lag = statistics.mean(consumer_lag_samples) if consumer_lag_samples else 0.0

        # Rate-relative threshold: configurable via STRESS_SAT_RATIO (default 0.5).
        # lag > rate * SAT_RATIO means Spark is more than SAT_RATIO seconds behind.
        # Floor at 50 to absorb normal jitter.
        sat_threshold = max(50.0, rate * STRESS_SAT_RATIO)

        # Secondary signal: lag growing monotonically over the last 3 samples
        trending_up = (
            len(consumer_lag_samples) >= 3
            and all(
                consumer_lag_samples[i] < consumer_lag_samples[i + 1]
                for i in range(len(consumer_lag_samples) - 3, len(consumer_lag_samples) - 1)
            )
        )
        saturating = avg_consumer_lag > sat_threshold or (avg_consumer_lag > 20 and trending_up)

        result = {
            "rate":               rate,
            "avg_consumer_lag":   round(avg_consumer_lag, 1),
            "sat_threshold":      sat_threshold,
            "avg_batch_ms":       int(statistics.mean(batch_samples))         if batch_samples         else 0,
            "avg_inject_rate":    round(statistics.mean(inject_samples), 1)    if inject_samples        else 0.0,
            "avg_mongo_rate":     round(statistics.mean(mongo_rate_samples), 1) if mongo_rate_samples   else 0.0,
            "avg_kafka_rate":     round(statistics.mean(kafka_rate_samples), 1) if kafka_rate_samples   else 0.0,
            "saturating":         saturating,
        }
        with _stress_lock:
            _stress_state["results"].append(result)
            if saturating and _stress_state["saturation_rate"] is None:
                _stress_state["saturation_rate"] = rate

    # Done — stop injection
    with _lock:
        _state["running"] = False
        _state["start_ts"] = None
    with _stress_lock:
        _stress_state["running"] = False
        _stress_state["phase"]   = "done"


_ft_state: dict = {
    "phase": "idle",       # idle | running | recovered | failed
    "scenario": None,
    "fault_start": None,
    "recovery_time_s": None,
    "baseline_mysql": 0,
    "baseline_mongo": 0,
    "after_mysql": 0,
    "after_mongo": 0,
    "timeline": [],
}
_ft_lock = threading.Lock()

_SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
    "redis.clients:jedis:5.1.0"
)

# Current Spark resource config — modified via /api/spark/resubmit
_spark_config: dict = {
    "total_executor_cores": None,   # None = use all available (default)
    "executor_memory":      None,   # None = worker default (e.g. "2g")
}
_spark_config_lock = threading.Lock()


def _build_spark_submit(extra_args: str = "") -> list:
    with _spark_config_lock:
        cores  = _spark_config["total_executor_cores"]
        memory = _spark_config["executor_memory"]
    cmd = [
        "docker", "exec", "-d", "cdc-spark-master",
        "/opt/spark/bin/spark-submit",
        "--class",  "CdcRedisConsumer",
        "--master", "spark://cdc-spark-master:7077",
        "--packages", _SPARK_PACKAGES,
    ]
    if cores:  cmd += ["--total-executor-cores", str(cores)]
    if memory: cmd += ["--executor-memory", memory]
    cmd.append("/opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar")
    return cmd


def _ft_log(event: str, detail: str, level: str = "info"):
    ts = time.strftime("%H:%M:%S")
    entry = {"ts": ts, "event": event, "detail": detail, "level": level}
    with _ft_lock:
        _ft_state["timeline"].append(entry)
    print(f"[FT] {ts} [{level.upper()}] {event}: {detail}")


def _docker(cmd: str, timeout: int = 30) -> bool:
    try:
        subprocess.run(cmd.split(), capture_output=True, timeout=timeout)
        return True
    except Exception as e:
        _ft_log("ERROR", f"docker cmd failed: {e}", "error")
        return False


def _container_status(name: str) -> str:
    try:
        r = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", name],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip()
    except Exception:
        return "unknown"


def _mysql_count() -> int:
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True, connect_timeout=3,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM customers")
        n = cur.fetchone()[0]
        conn.close()
        return n
    except Exception:
        return -1


def _mongo_count() -> int:
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        return client["inventory"]["customers"].count_documents({})
    except Exception:
        return -1


def _insert_ft_record(label: str) -> bool:
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True, connect_timeout=3,
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
            (f"FT-{label}", f"ft_{label.lower()}@test.com", "0900000099"),
        )
        conn.close()
        return True
    except Exception as e:
        _ft_log("DATA", f"Insert during fault failed: {e}", "warn")
        return False


def _wait_sync(expected: int, timeout: int = 60) -> bool:
    """Wait until MongoDB count >= expected. Returns True if converged."""
    for _ in range(timeout // 2):
        time.sleep(2)
        mc = _mongo_count()
        if mc >= expected:
            return True
    return False


def _run_kafka_scenario():
    with _ft_lock:
        _ft_state["phase"] = "running"
        _ft_state["timeline"] = []

    _ft_log("SCENARIO", "Kafka Broker Crash & Recovery", "info")

    before_mysql = _mysql_count()
    before_mongo = _mongo_count()
    with _ft_lock:
        _ft_state["baseline_mysql"] = before_mysql
        _ft_state["baseline_mongo"] = before_mongo

    _ft_log("BASELINE", f"MySQL={before_mysql} | MongoDB={before_mongo}", "info")
    _ft_log("INJECT", "Stopping cdc-kafka container…", "warn")

    _docker("docker stop cdc-kafka")
    _ft_log("STATUS", "Kafka DOWN — pipeline paused", "error")

    time.sleep(3)
    if _insert_ft_record("Kafka"):
        _ft_log("DATA", "1 record inserted into MySQL while Kafka is DOWN", "info")

    time.sleep(5)
    _ft_log("RECOVER", "Starting cdc-kafka container…", "info")
    fault_start = time.time()

    _docker("docker start cdc-kafka")

    # Wait for Kafka to be running
    for _ in range(15):
        time.sleep(2)
        if _container_status("cdc-kafka") == "running":
            _ft_log("STATUS", "Kafka container running — Debezium reconnecting…", "info")
            break

    # Wait for Spark to drain Kafka backlog (2 trigger cycles = 10s)
    time.sleep(15)
    recovery_s = int(time.time() - fault_start)

    after_mysql = _mysql_count()
    after_mongo = _mongo_count()

    with _ft_lock:
        _ft_state["after_mysql"] = after_mysql
        _ft_state["after_mongo"] = after_mongo
        _ft_state["recovery_time_s"] = recovery_s
        _ft_state["phase"] = "recovered" if after_mongo >= after_mysql else "failed"

    lost = max(0, after_mysql - after_mongo)
    status = "PASS — 0 data loss" if lost == 0 else f"LAG — {lost} records still processing"
    level = "success" if lost == 0 else "warn"
    _ft_log("RESULT", f"Recovery: {recovery_s}s | MySQL={after_mysql} | MongoDB={after_mongo} | {status}", level)


def _run_debezium_scenario():
    with _ft_lock:
        _ft_state["phase"] = "running"
        _ft_state["timeline"] = []

    _ft_log("SCENARIO", "Debezium Connector Restart & Offset Recovery", "info")

    before_mysql = _mysql_count()
    before_mongo = _mongo_count()
    with _ft_lock:
        _ft_state["baseline_mysql"] = before_mysql
        _ft_state["baseline_mongo"] = before_mongo

    _ft_log("BASELINE", f"MySQL={before_mysql} | MongoDB={before_mongo}", "info")
    _ft_log("INJECT", "Restarting cdc-debezium container…", "warn")

    _docker("docker restart cdc-debezium")
    _ft_log("STATUS", "Debezium DOWN — CDC paused (binlog offset stored)", "error")

    time.sleep(5)
    if _insert_ft_record("Debezium"):
        _ft_log("DATA", "1 record inserted while Debezium is restarting", "info")
    _ft_log("STATUS", "MySQL binlog recorded the change — will be replayed on reconnect", "info")

    fault_start = time.time()
    # Wait for Debezium to come back up
    for _ in range(20):
        time.sleep(2)
        if _container_status("cdc-debezium") == "running":
            _ft_log("STATUS", "Debezium container running — resuming from stored binlog offset", "info")
            break

    time.sleep(15)  # Let Debezium publish caught-up events + Spark process them
    recovery_s = int(time.time() - fault_start)

    after_mysql = _mysql_count()
    after_mongo = _mongo_count()
    with _ft_lock:
        _ft_state["after_mysql"] = after_mysql
        _ft_state["after_mongo"] = after_mongo
        _ft_state["recovery_time_s"] = recovery_s
        _ft_state["phase"] = "recovered" if after_mongo >= after_mysql else "failed"

    lost = max(0, after_mysql - after_mongo)
    status = "PASS — binlog offset preserved, 0 events lost" if lost == 0 else f"LAG — {lost} records still syncing"
    level = "success" if lost == 0 else "warn"
    _ft_log("RESULT", f"Recovery: {recovery_s}s | MySQL={after_mysql} | MongoDB={after_mongo} | {status}", level)


def _run_spark_scenario():
    with _ft_lock:
        _ft_state["phase"] = "running"
        _ft_state["timeline"] = []

    _ft_log("SCENARIO", "Spark Job Crash & Checkpoint Recovery", "info")

    before_mysql = _mysql_count()
    before_mongo = _mongo_count()
    with _ft_lock:
        _ft_state["baseline_mysql"] = before_mysql
        _ft_state["baseline_mongo"] = before_mongo

    _ft_log("BASELINE", f"MySQL={before_mysql} | MongoDB={before_mongo}", "info")
    _ft_log("INJECT", "Killing Spark streaming job (CdcRedisConsumer)…", "warn")

    subprocess.run(
        ["docker", "exec", "cdc-spark-master", "pkill", "-f", "CdcRedisConsumer"],
        capture_output=True, timeout=10,
    )
    _ft_log("STATUS", "Spark job DOWN — messages accumulating in Kafka", "error")
    _ft_log("INFO", "Checkpoint at /tmp/spark-checkpoint/cdc-pipeline is preserved", "info")

    time.sleep(3)
    if _insert_ft_record("Spark"):
        _ft_log("DATA", "1 record in MySQL → event in Kafka → waiting for Spark restart", "info")

    time.sleep(5)
    _ft_log("RECOVER", "Re-submitting Spark job with existing checkpoint…", "info")
    fault_start = time.time()

    subprocess.run(_build_spark_submit(), capture_output=True, timeout=15)
    _ft_log("STATUS", "Spark job re-submitted — reading from checkpoint offset, no reprocessing", "info")

    # Wait for Spark packages download + first batch (can take ~30-90s on first submit after recreate)
    time.sleep(30)
    _wait_sync(before_mysql + 1, timeout=120)
    recovery_s = int(time.time() - fault_start)

    after_mysql = _mysql_count()
    after_mongo = _mongo_count()
    with _ft_lock:
        _ft_state["after_mysql"] = after_mysql
        _ft_state["after_mongo"] = after_mongo
        _ft_state["recovery_time_s"] = recovery_s
        _ft_state["phase"] = "recovered" if after_mongo >= after_mysql else "failed"

    lost = max(0, after_mysql - after_mongo)
    status = "PASS — checkpoint prevents duplicates, 0 data loss" if lost == 0 else f"LAG — {lost} still in Kafka"
    level = "success" if lost == 0 else "warn"
    _ft_log("RESULT", f"Recovery: {recovery_s}s | MySQL={after_mysql} | MongoDB={after_mongo} | {status}", level)


# ── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=str(DEMO_DIR))

@app.route("/")
def index():
    return send_from_directory(DEMO_DIR, "index.html")

# ── Status ───────────────────────────────────────────────────────────
@app.route("/api/status")
def api_status():
    with _lock:
        s = {k: v for k, v in _state.items() if k != "thread"}
        elapsed = int(time.time() - s["start_ts"]) if s["start_ts"] else 0
    return jsonify({
        **s,
        "elapsed_s": elapsed,
        "engine": _detect_engine(),
        "config": {
            "mysql": f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}",
            "redis": f"{REDIS_HOST}:{REDIS_PORT}",
            "mongo": MONGO_URI,
            "prom":  PROM_URL,
        },
    })

# ── Load control ─────────────────────────────────────────────────────
@app.route("/api/start", methods=["POST"])
def api_start():
    rate = int(request.args.get("rate", 100))
    with _lock:
        if _state["running"]:
            return jsonify({"ok": False, "reason": "already running"})
        _state.update(running=True, rate=rate, injected=0, errors=0,
                      start_ts=time.time())
        t = threading.Thread(target=_load_worker, args=(rate,), daemon=True)
        _state["thread"] = t
    t.start()
    return jsonify({"ok": True, "rate": rate})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    with _lock:
        _state["running"]  = False
        _state["start_ts"] = None
    return jsonify({"ok": True, "injected": _state["injected"]})

# ── Data queries ─────────────────────────────────────────────────────
@app.route("/api/redis")
def api_redis():
    try:
        r = redis_lib.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            decode_responses=True, socket_timeout=2,
        )
        top_raw = r.zrevrange("top_customers:order_count", 0, 4, withscores=True)
        batch_dur = r.get("spark:batch_duration_ms")
        return jsonify({
            "ok":               True,
            "customers_total":  int(r.get("customers:total") or 0),
            "orders_total":     int(r.get("orders:total") or 0),
            "orders_revenue":   float(r.get("orders:revenue") or 0),
            "batch_duration_ms": int(batch_dur) if batch_dur else None,
            "top_customers":    [{"id": k, "score": int(v)} for k, v in top_raw],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/mongo")
def api_mongo():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db     = client["inventory"]
        count  = db["customers"].count_documents({})
        recent = list(
            db["customers"]
            .find({}, {"_id": 0, "id": 1, "name": 1, "email": 1, "phone": 1})
            .sort("id", -1)
            .limit(8)
        )
        return jsonify({"ok": True, "count": count, "recent": recent})
    except Exception as e:
        return jsonify({"ok": False, "count": 0, "recent": [], "error": str(e)})

@app.route("/api/orders")
def api_orders():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db     = client["inventory"]
        count  = db["orders"].count_documents({})
        recent = list(
            db["orders"]
            .find({}, {"_id": 0, "id": 1, "customer_id": 1, "total_amount": 1, "status": 1})
            .sort("id", -1)
            .limit(8)
        )
        return jsonify({"ok": True, "count": count, "recent": recent})
    except Exception as e:
        return jsonify({"ok": False, "count": 0, "recent": [], "error": str(e)})

@app.route("/api/clear", methods=["POST"])
def api_clear():
    errors = []
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True, charset="utf8mb4",
        )
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute("TRUNCATE TABLE orders")
        cur.execute("TRUNCATE TABLE customers")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.close()
    except Exception as e:
        errors.append(f"mysql: {e}")

    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db = client["inventory"]
        db["customers"].delete_many({})
        db["orders"].delete_many({})
    except Exception as e:
        errors.append(f"mongo: {e}")

    try:
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=2)
        r.flushdb()
    except Exception as e:
        errors.append(f"redis: {e}")

    # Kafka intentionally not cleared — offset stays to show fault-tolerance:
    # data persists in Kafka even after MySQL/Mongo/Redis are wiped.
    return jsonify({"ok": len(errors) == 0, "errors": errors})

@app.route("/api/comparison")
def api_comparison():
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True, charset="utf8mb4",
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM customers")
        mysql_count = cur.fetchone()[0]
        cur.execute(
            "SELECT id, name, email, phone, created_at "
            "FROM customers ORDER BY id DESC LIMIT 8"
        )
        rows = cur.fetchall()
        conn.close()

        mysql_records = [
            {"id": r[0], "name": r[1], "email": r[2], "phone": r[3],
             "created_at": r[4].strftime("%Y-%m-%d %H:%M") if r[4] else None}
            for r in rows
        ]
        ids = [r[0] for r in rows]

        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db = client["inventory"]
        mongo_by_id = {}
        for mid in ids:
            doc = db["customers"].find_one({"_id": mid}, {"_id": 1, "name": 1, "email": 1})
            if doc is None:
                doc = db["customers"].find_one({"_id": int(mid)}, {"_id": 1, "name": 1, "email": 1})
            if doc:
                mongo_by_id[mid] = {"id": doc["_id"], "name": doc.get("name"), "email": doc.get("email")}

        return jsonify({"ok": True, "mysql_count": mysql_count, "mysql": mysql_records, "mongo_by_id": mongo_by_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "mysql_count": 0, "mysql": [], "mongo": []})


def _kafka_partition_count(topic: str) -> int:
    """Query partition count of a Kafka topic via docker exec (no extra deps)."""
    try:
        r = subprocess.run(
            ["docker", "exec", "cdc-kafka", "/bin/kafka-topics",
             "--describe", "--topic", topic, "--bootstrap-server", "localhost:9092"],
            capture_output=True, text=True, timeout=6,
        )
        for line in r.stdout.splitlines():
            if "PartitionCount:" in line:
                return int(line.split("PartitionCount:")[1].split()[0])
    except Exception:
        pass
    return -1


def _spark_info() -> dict:
    """Fetch worker/executor info from Spark Master REST API."""
    for url in [f"http://{os.getenv('SPARK_MASTER_HOST', 'localhost')}:8080/json/",
                "http://localhost:8080/json/"]:
        try:
            with urllib.request.urlopen(urllib.request.Request(url), timeout=3) as r:
                data = json.loads(r.read())
            workers = [w for w in data.get("workers", []) if w.get("state") == "ALIVE"]
            return {
                "worker_count":  len(workers),
                "cores_per_worker": workers[0].get("cores", 0) if workers else 0,
                "mem_per_worker_mb": workers[0].get("memory", 0) if workers else 0,
                "total_cores": sum(w.get("cores", 0) for w in workers),
            }
        except Exception:
            continue
    return {"worker_count": 0, "cores_per_worker": 0, "mem_per_worker_mb": 0, "total_cores": 0}


@app.route("/api/test_config")
def api_test_config():
    """Return current hardware/pipeline config so UI can show what's being tested."""
    customers_parts = _kafka_partition_count("inventory.inventory.customers")
    orders_parts    = _kafka_partition_count("inventory.inventory.orders")
    spark           = _spark_info()
    return jsonify({
        "kafka": {
            "customers_partitions": customers_parts,
            "orders_partitions":    orders_parts,
        },
        "spark": spark,
        "stress": {
            "rates":      STRESS_RATES,
            "duration_s": STRESS_DURATION,
            "sat_ratio":  STRESS_SAT_RATIO,
        },
    })


def _detect_engine() -> str:
    for url in [f"http://{os.getenv('SPARK_MASTER_HOST', 'localhost')}:8080/json/",
                "http://localhost:8080/json/"]:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=2) as r:
                data = json.loads(r.read())
            apps = data.get("activeapps", [])
            if not apps:
                continue
            name = apps[0].get("name", "")
            if "CDC-MySQL-To-MongoDB-Redis" in name:
                return "scala"
            if "Pipeline" in name or "python" in name.lower():
                return "python"
        except Exception:
            continue
    return "unknown"

@app.route("/api/metrics")
def api_metrics():
    wanted = [
        "cdc_mysql_customers_total",
        "cdc_mongo_customers_total",
        "cdc_kafka_customers_offset",
        "cdc_redis_customers_total",
        "cdc_mysql_insert_rate",
        "cdc_mongo_write_rate",
        "cdc_kafka_rate_total",
        "cdc_lag_total",
        "cdc_spark_batch_duration_ms",
        "cdc_spark_executor_cores",
        "cdc_spark_executor_memory_mb",
    ]
    result = {}
    for m in wanted:
        try:
            url = f"{PROM_URL}/api/v1/query?query={m}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read())
            rs = data.get("data", {}).get("result", [])
            result[m] = float(rs[0]["value"][1]) if rs else 0.0
        except Exception:
            result[m] = None

    # Return relative offsets (0 after clear) while Prometheus keeps absolute values
    if result.get("cdc_kafka_customers_offset") is not None:
        result["cdc_kafka_customers_offset"] = max(0.0, result["cdc_kafka_customers_offset"] - _kafka_base["customers"])

    return jsonify(result)


# ── Fault Tolerance API ──────────────────────────────────────────────
@app.route("/api/ft/health")
def api_ft_health():
    containers = [
        "cdc-mysql", "cdc-debezium", "cdc-kafka",
        "cdc-spark-master", "cdc-mongodb", "cdc-redis", "cdc-zookeeper",
    ]
    return jsonify({
        "ok": True,
        "containers": {c: _container_status(c) for c in containers},
        "ts": time.time(),
    })


@app.route("/api/ft/state")
def api_ft_state():
    with _ft_lock:
        state = {k: v for k, v in _ft_state.items()}
    state["containers"] = {
        c: _container_status(c)
        for c in ["cdc-mysql", "cdc-debezium", "cdc-kafka", "cdc-spark-master"]
    }
    return jsonify(state)


@app.route("/api/ft/inject", methods=["POST"])
def api_ft_inject():
    body = request.get_json(silent=True) or {}
    scenario = body.get("scenario", "kafka")
    with _ft_lock:
        if _ft_state["phase"] == "running":
            return jsonify({"ok": False, "reason": "scenario already running"})
        _ft_state.update(
            scenario=scenario, phase="idle",
            fault_start=None, recovery_time_s=None,
            baseline_mysql=0, baseline_mongo=0,
            after_mysql=0, after_mongo=0, timeline=[],
        )

    runners = {
        "kafka":    _run_kafka_scenario,
        "debezium": _run_debezium_scenario,
        "spark":    _run_spark_scenario,
    }
    fn = runners.get(scenario)
    if fn is None:
        return jsonify({"ok": False, "reason": f"unknown scenario: {scenario}"})

    threading.Thread(target=fn, daemon=True).start()
    return jsonify({"ok": True, "scenario": scenario})


# ── Spark resource config API ────────────────────────────────────────
@app.route("/api/spark/config", methods=["GET"])
def api_spark_config():
    with _spark_config_lock:
        cfg = dict(_spark_config)
    cfg.update(_spark_info())
    return jsonify({"ok": True, **cfg})


@app.route("/api/spark/resubmit", methods=["POST"])
def api_spark_resubmit():
    """Kill current Spark job and resubmit with new resource config."""
    body = request.get_json(silent=True) or {}
    cores  = body.get("total_executor_cores")   # int or null
    memory = body.get("executor_memory")         # "2g" / "4g" or null

    with _spark_config_lock:
        _spark_config["total_executor_cores"] = int(cores)  if cores  else None
        _spark_config["executor_memory"]      = str(memory) if memory else None

    # Kill existing job
    subprocess.run(
        ["docker", "exec", "cdc-spark-master", "pkill", "-f", "CdcRedisConsumer"],
        capture_output=True, timeout=10,
    )
    time.sleep(2)

    # Resubmit with new params
    cmd = _build_spark_submit()
    subprocess.run(cmd, capture_output=True, timeout=15)

    with _spark_config_lock:
        applied = dict(_spark_config)
    return jsonify({"ok": True, "applied": applied, "cmd": " ".join(cmd)})


@app.route("/api/ft/reset", methods=["POST"])
def api_ft_reset():
    with _ft_lock:
        _ft_state.update(
            phase="idle", scenario=None, fault_start=None,
            recovery_time_s=None, baseline_mysql=0, baseline_mongo=0,
            after_mysql=0, after_mongo=0, timeline=[],
        )
    return jsonify({"ok": True})


@app.route("/api/insert_one", methods=["POST"])
def api_insert_one():
    body  = request.get_json(silent=True) or {}
    name  = body.get("name",  "Demo User")
    email = body.get("email", "") or f"demo{int(time.time())}@test.com"
    phone = body.get("phone", "0900000000")

    t0 = time.time()
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True, charset="utf8mb4",
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
            (name, email, phone),
        )
        new_id   = cur.lastrowid
        mysql_ms = int((time.time() - t0) * 1000)
        conn.close()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

    # Poll MongoDB until record appears (max 15s, check every 200ms)
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    db     = client["inventory"]
    mongo_ms = None
    deadline = time.time() + 15
    while time.time() < deadline:
        doc = db["customers"].find_one({"_id": new_id})
        if doc is None:
            doc = db["customers"].find_one({"_id": int(new_id)})
        if doc:
            mongo_ms = int((time.time() - t0) * 1000)
            break
        time.sleep(0.2)

    return jsonify({
        "ok":       True,
        "id":       new_id,
        "name":     name,
        "email":    email,
        "mysql_ms": mysql_ms,
        "mongo_ms": mongo_ms,
        "e2e_ms":   mongo_ms,
        "synced":   mongo_ms is not None,
    })


@app.route("/api/update_order", methods=["POST"])
def api_update_order():
    body       = request.get_json(silent=True) or {}
    order_id   = body.get("id")
    new_status = body.get("status")
    valid      = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}
    if new_status not in valid:
        return jsonify({"ok": False, "reason": "invalid status"})
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB,
            autocommit=True,
        )
        cur = conn.cursor()
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", (new_status, order_id))
        affected = cur.rowcount
        conn.close()
        return jsonify({"ok": True, "affected": affected, "new_status": new_status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Capacity Stress Test API ─────────────────────────────────────────
@app.route("/api/stress/start", methods=["POST"])
def api_stress_start():
    with _stress_lock:
        if _stress_state["running"]:
            return jsonify({"ok": False, "reason": "already running"})
        _stress_state.update(
            running=True, phase="running",
            current_level=-1, current_rate=0,
            results=[], saturation_rate=None,
        )
    threading.Thread(target=_stress_worker, daemon=True).start()
    return jsonify({"ok": True, "levels": STRESS_RATES, "duration_s": STRESS_DURATION, "sat_ratio": STRESS_SAT_RATIO})


@app.route("/api/stress/stop", methods=["POST"])
def api_stress_stop():
    with _stress_lock:
        _stress_state["running"] = False
        _stress_state["phase"]   = "idle"
    with _lock:
        _state["running"] = False
        _state["start_ts"] = None
    return jsonify({"ok": True})


@app.route("/api/stress/state", methods=["GET"])
def api_stress_state():
    with _stress_lock:
        return jsonify(dict(_stress_state))


if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════╗
║        CDC Data Pipeline — Live Demo Server          ║
╚══════════════════════════════════════════════════════╝

  Dashboard : http://localhost:{DEMO_PORT}
  MySQL     : {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}
  Redis     : {REDIS_HOST}:{REDIS_PORT}
  MongoDB   : {MONGO_URI}
  Prometheus: {PROM_URL}

  Tip: Copy .env.example → .env để đổi host cho máy khác
""")
    app.run(host="0.0.0.0", port=DEMO_PORT, debug=False, threaded=True)
