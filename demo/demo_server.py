#!/usr/bin/env python3
"""
CDC Data Pipeline — Live Demo Server
Run  : python demo_server.py   (sau khi: pip install -r requirements.txt)
Open : http://localhost:8888
"""

import os, json, time, threading, random
from pathlib import Path

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
    # batch_size nhỏ → nhiều batches/s, cap ở 50 để không quá tải
    batch_size = max(1, min(rate // 5, 50))
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
    return jsonify(result)


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
