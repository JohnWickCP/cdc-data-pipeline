#!/usr/bin/env python3
"""
Full Pipeline Crash Test - CDC Data Pipeline
Scenario: Batch A -> crash Kafka+Workers -> Batch B -> recover -> verify no data loss
MySQL schema: customers(id, name, email, phone, created_at)
"""
import subprocess, time, sys, datetime, json, os, tempfile

MYSQL_CONTAINER = "cdc-mysql"
MONGO_CONTAINER = "cdc-mongodb"
BATCH_SIZE       = 50
WAIT_PROPAGATION = 40  # seconds
WAIT_RECOVERY    = 70  # seconds

RESULTS_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "benchmark", "results", "fault_tolerance_results.jsonl")

now_tag    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
TEST_TAG   = f"ft_{now_tag}"
_test_start = time.time()
_timings   = {}  # phase -> elapsed seconds

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"
WARN = "\033[93m[WARN]\033[0m"
results = []

# ---- subprocess helpers (NO shell=True, NO bash dependency) ----

def run_list(args, timeout=45):
    """Run a command as a list — safe on any OS, no shell quoting issues."""
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip(), r.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", 1

def mysql_query(sql):
    """Execute SQL against MySQL container directly (no shell escaping)."""
    out, rc = run_list([
        "docker", "exec", MYSQL_CONTAINER,
        "mysql", "-uroot", "-proot", "inventory", "-e", sql
    ])
    lines = [l for l in out.splitlines() if "Warning" not in l and l.strip()]
    return lines, rc

def mysql_count(table):
    lines, rc = mysql_query(f"SELECT COUNT(*) FROM {table};")
    for l in lines:
        if l.strip().isdigit():
            return int(l.strip()), rc
    return -1, rc

def mysql_insert(sql):
    """Write SQL to a temp file, docker cp into container, execute via sh -c."""
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8')
    tmp.write(sql)
    tmp.close()
    try:
        _, rc1 = run_list(["docker", "cp", tmp.name, f"{MYSQL_CONTAINER}:/tmp/_ft_insert.sql"])
        if rc1 != 0:
            return f"docker cp failed rc={rc1}", rc1
        out, rc2 = run_list([
            "docker", "exec", MYSQL_CONTAINER,
            "sh", "-c", "mysql -uroot -proot inventory < /tmp/_ft_insert.sql 2>&1"
        ])
        return out, rc2
    finally:
        os.unlink(tmp.name)

def mongo_count_tag(name_fragment, collection="customers"):
    """Count docs where name matches the fragment (email is masked in MongoDB, use name instead)."""
    js = (f"db=db.getSiblingDB('inventory');"
          f"print(db.{collection}.countDocuments({{name: /{name_fragment}/}}));")
    out, rc = run_list([
        "docker", "exec", MONGO_CONTAINER,
        "mongosh", "--quiet", "--eval", js
    ])
    for line in out.splitlines():
        l = line.strip()
        if l.isdigit():
            return int(l), rc
    return -1, rc

def docker_stop(*containers):
    return run_list(["docker", "stop"] + list(containers), timeout=60)

def docker_start(*containers):
    return run_list(["docker", "start"] + list(containers), timeout=30)

def docker_inspect_health(container):
    out, rc = run_list([
        "docker", "inspect", container,
        "--format", "{{.State.Health.Status}}"
    ])
    return out.strip(), rc

def docker_ps_names():
    """Return a set of running container names (exact match, no substrings)."""
    out, _ = run_list(["docker", "ps", "--format", "{{.Names}}"])
    return set(line.strip() for line in out.splitlines() if line.strip())

# ---- UI ----

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def check(label, cond, detail=""):
    status = PASS if cond else FAIL
    mark = "v" if cond else "x"
    line = f"  {status} [{mark}] {label}"
    if detail:
        line += f"  ({detail})"
    print(line)
    results.append((label, cond))
    return cond

def wait_prog(seconds, msg):
    print(f"  {INFO} {msg} ", end="", flush=True)
    for i in range(seconds):
        time.sleep(1)
        remaining = seconds - i - 1
        if remaining > 0 and remaining % 10 == 0:
            print(f"{remaining}s ", end="", flush=True)
    print("done")

# ============================================================
# PHASE 0: Baseline
# ============================================================
section("PHASE 0 — Baseline state")

base_mysql, _ = mysql_count("customers")
base_mongo, _ = mongo_count_tag("FaultTest", "customers")
print(f"  MySQL  customers : {base_mysql}")
print(f"  MongoDB customers: {base_mongo}")
check("Pipeline has existing data in MongoDB", base_mongo > 0, f"{base_mongo} docs")

# ============================================================
# PHASE 1: Batch A — before crash
# ============================================================
section("PHASE 1 — Insert Batch A (pipeline RUNNING)")

rows_a = [f"('{TEST_TAG}_a_{i}@test.com', 'FA_{TEST_TAG}_{i}')" for i in range(BATCH_SIZE)]
sql_a  = "INSERT INTO customers (email, name) VALUES " + ", ".join(rows_a) + ";"
out, rc = mysql_insert(sql_a)
check("Batch A inserted into MySQL", rc == 0,
      f"{BATCH_SIZE} records, name=FA_{TEST_TAG}")
if rc != 0:
    print(f"  {WARN} Error: {out[:300]}")

wait_prog(WAIT_PROPAGATION, f"Waiting {WAIT_PROPAGATION}s for Spark (5s trigger + buffer)...")

mongo_a, _ = mongo_count_tag(f"FA_{TEST_TAG}", "customers")
ok_a = check("Batch A propagated to MongoDB", mongo_a == BATCH_SIZE,
             f"expected {BATCH_SIZE}, got {mongo_a}")
if not ok_a and mongo_a >= 0:
    print(f"  {WARN} Partial propagation ({mongo_a}) — waiting 20s more...")
    wait_prog(20, "Extra wait...")
    mongo_a, _ = mongo_count_tag(f"FA_{TEST_TAG}", "customers")
    check("Batch A propagated (2nd try)", mongo_a == BATCH_SIZE, f"got {mongo_a}")

# ============================================================
# PHASE 2: CRASH — stop Kafka + Spark workers
# ============================================================
section("PHASE 2 — CRASH: Stop Kafka + all Spark Workers")
_timings["crash_start"] = time.time()

print(f"  {WARN} Stopping cdc-kafka + 3 spark workers ...")
out, rc = docker_stop(
    "cdc-spark-worker-1", "cdc-spark-worker-2", "cdc-spark-worker-3", "cdc-kafka"
)
check("docker stop completed", rc == 0, out[:80].replace("\n", " "))

time.sleep(3)
running = docker_ps_names()  # set of exact container names
check("Kafka confirmed DOWN",         "cdc-kafka" not in running)
check("Spark workers confirmed DOWN", "cdc-spark-worker-1" not in running)

# ============================================================
# PHASE 3: Batch B — while pipeline is down
# ============================================================
section("PHASE 3 — Insert Batch B (pipeline DOWN)")

rows_b = [f"('{TEST_TAG}_b_{i}@test.com', 'FB_{TEST_TAG}_{i}')" for i in range(BATCH_SIZE)]
sql_b  = "INSERT INTO customers (email, name) VALUES " + ", ".join(rows_b) + ";"
out, rc = mysql_insert(sql_b)
check("Batch B inserted into MySQL while pipeline DOWN", rc == 0, f"{BATCH_SIZE} records")
if rc != 0:
    print(f"  {WARN} Error: {out[:300]}")

time.sleep(4)
mongo_b_early, _ = mongo_count_tag(f"FB_{TEST_TAG}", "customers")
check("Batch B NOT in MongoDB yet (expected — pipeline down)", mongo_b_early == 0,
      f"found {mongo_b_early} (should be 0)")

# ============================================================
# PHASE 4: RECOVER — restart in order
# ============================================================
section("PHASE 4 — RECOVER: Start Kafka then Spark Workers")
_timings["recover_start"] = time.time()
_timings["downtime_s"] = round(_timings["recover_start"] - _timings["crash_start"])

print(f"  {INFO} Starting cdc-kafka ...")
docker_start("cdc-kafka")
wait_prog(28, "Waiting for Kafka to become healthy...")

hlt, _ = docker_inspect_health("cdc-kafka")
check("Kafka healthy after restart", "healthy" in hlt.lower(), hlt)

print(f"  {INFO} Starting cdc-spark-worker-1/2/3 ...")
docker_start("cdc-spark-worker-1", "cdc-spark-worker-2", "cdc-spark-worker-3")
wait_prog(20, "Waiting for workers to register with master...")

spark_json, _ = run_list(["curl", "-s", "http://localhost:8080/json/"], timeout=10)
try:
    sd = json.loads(spark_json)
    alive = sd.get("aliveworkers", 0)   # int in Spark 3.x
    apps  = sd.get("activeapps", [])    # list
    check("Spark workers alive", alive >= 1, f"{alive} workers")
    app_ok = len(apps) > 0
    check("CDC Spark app still RUNNING", app_ok,
          apps[0].get("name", "?") if app_ok else "no active app — driver may need resubmit")
except Exception as e:
    check("Spark master API reachable", False, str(e)[:100])

# ============================================================
# PHASE 5: Wait + Verify recovery
# ============================================================
section("PHASE 5 — Wait for Batch B recovery")

wait_prog(WAIT_RECOVERY,
          f"Waiting {WAIT_RECOVERY}s (Debezium reads binlog from last offset + Spark trigger)...")

_timings["recovery_confirmed"] = time.time()
_timings["recovery_total_s"] = round(_timings["recovery_confirmed"] - _timings["recover_start"])

mongo_b_final, _ = mongo_count_tag(f"FB_{TEST_TAG}", "customers")
check("Batch B recovered in MongoDB", mongo_b_final == BATCH_SIZE,
      f"expected {BATCH_SIZE}, got {mongo_b_final}")

mongo_a_final, _ = mongo_count_tag(f"FA_{TEST_TAG}", "customers")
check("Batch A intact — no duplicates", mongo_a_final == BATCH_SIZE,
      f"found {mongo_a_final}")

total = mongo_a_final + mongo_b_final
check("Zero data loss (A + B = 100 records total)", total == BATCH_SIZE * 2,
      f"{total}/100")

# ============================================================
# SUMMARY
# ============================================================
section("SUMMARY")

passed = sum(1 for _, ok in results if ok)
failed = len(results) - passed

for label, ok in results:
    c = "\033[92m" if ok else "\033[91m"
    m = "v" if ok else "x"
    print(f"  {c}[{m}]\033[0m {label}")

verdict = "\033[92mALL PASSED\033[0m" if failed == 0 else f"\033[91m{failed} FAILED\033[0m"
print(f"\n  Result: {passed}/{len(results)} — {verdict}")

if failed == 0:
    print("\n  Conclusion: Pipeline dat kha nang chiu loi.")
    print("  Kafka + Spark workers crash -> Debezium giu binlog offset ->")
    print("  data replay sau khi recover -> KHONG mat record, KHONG duplicate.")
else:
    print("\n  Conclusion: Co van de. Xem chi tiet o tren.")

# ============================================================
# SAVE RESULTS
# ============================================================
record = {
    "timestamp":       datetime.datetime.now().isoformat(),
    "test_tag":        TEST_TAG,
    "scenario":        "kafka_spark_workers_crash",
    "batch_size":      BATCH_SIZE,
    "total_records":   BATCH_SIZE * 2,
    "records_recovered": total if 'total' in dir() else -1,
    "data_loss":       max(0, BATCH_SIZE * 2 - (total if 'total' in dir() else 0)),
    "duplicates_batch_a": max(0, (mongo_a_final if 'mongo_a_final' in dir() else 0) - BATCH_SIZE),
    "downtime_s":      _timings.get("downtime_s", -1),
    "recovery_total_s": _timings.get("recovery_total_s", -1),
    "wait_propagation_s": WAIT_PROPAGATION,
    "wait_recovery_s":  WAIT_RECOVERY,
    "checks_passed":   passed,
    "checks_total":    len(results),
    "verdict":         "PASS" if failed == 0 else "FAIL",
    "checks": {label: ok for label, ok in results},
}
os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
with open(RESULTS_FILE, "a", encoding="utf-8") as f:
    f.write(json.dumps(record, ensure_ascii=False) + "\n")
print(f"\n  Results saved -> {RESULTS_FILE}")

sys.exit(0 if failed == 0 else 1)
