#!/usr/bin/env python3

"""
metrics_exporter.py
Thu thập metrics từ MySQL, MongoDB, Redis, Kafka
và benchmark JSON để Prometheus đọc.
"""

import os
from dotenv import load_dotenv

# Tải biến môi trường từ file .env nếu có
load_dotenv()

import time
import json
import pymysql
import pymongo
import redis

from pathlib import Path
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from prometheus_client import start_http_server, Gauge
from datetime import datetime


# =============================
# Config
# =============================

MYSQL_CONFIG = dict(
    host=os.environ.get("MYSQL_HOST", "localhost"),
    port=int(os.environ.get("MYSQL_PORT", 3306)),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", "root"),
    db=os.environ.get("MYSQL_DB", "inventory")
)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "http://cdc-spark-master:8080")

TOPICS = [
    "inventory.inventory.customers",
    "inventory.inventory.orders"
]


# =============================
# Benchmark JSON
# =============================

BENCHMARK_DIR = Path(os.environ.get("BENCHMARK_DIR", Path(__file__).parent.parent.parent / "benchmark"))
SCALABILITY_JSON = BENCHMARK_DIR / "results" / "scalability_report.json"
SCALING_JSON = BENCHMARK_DIR / "results" / "scaling_results.json"
LATEST_BENCHMARK_JSON = BENCHMARK_DIR / "results" / "latest_benchmark.json"
TPS_RESULTS_JSON = BENCHMARK_DIR / "results" / "tps_results.json"


# =============================
# Prometheus Metrics
# =============================

# MySQL
mysql_customers = Gauge(
    "cdc_mysql_customers_total",
    "Customers count in MySQL"
)

mysql_orders = Gauge(
    "cdc_mysql_orders_total",
    "Orders count in MySQL"
)


# MongoDB
mongo_customers = Gauge(
    "cdc_mongo_customers_total",
    "Customers count in MongoDB"
)

mongo_orders = Gauge(
    "cdc_mongo_orders_total",
    "Orders count in MongoDB"
)


# Redis
redis_orders_revenue = Gauge(
    "cdc_redis_orders_revenue",
    "Orders revenue in Redis"
)

redis_orders_total = Gauge(
    "cdc_redis_orders_total",
    "Orders total in Redis"
)

redis_customers_total = Gauge(
    "cdc_redis_customers_total",
    "Customer events total in Redis"
)

redis_top_customer_score = Gauge(
    "cdc_redis_top_customer_score",
    "Top customer score"
)


# Kafka
kafka_customers_offset = Gauge(
    "cdc_kafka_customers_offset",
    "Kafka customers topic offset"
)

kafka_orders_offset = Gauge(
    "cdc_kafka_orders_offset",
    "Kafka orders topic offset"
)


# Pipeline health
pipeline_up = Gauge(
    "cdc_pipeline_up",
    "Pipeline status"
)

mysql_mongo_in_sync = Gauge(
    "cdc_mysql_mongo_in_sync",
    "MySQL and MongoDB in sync"
)


# =============================
# Benchmark metrics
# =============================

bench_latency_p50 = Gauge("cdc_benchmark_latency_p50", "E2E latency p50 seconds")
bench_latency_p95 = Gauge("cdc_benchmark_latency_p95", "E2E latency p95 seconds")
bench_latency_p99 = Gauge("cdc_benchmark_latency_p99", "E2E latency p99 seconds")
bench_latency_avg = Gauge("cdc_benchmark_latency_avg", "E2E latency avg seconds")

bench_throughput_e2e = Gauge("cdc_benchmark_throughput_e2e", "E2E throughput records/s")
bench_throughput_mysql = Gauge("cdc_benchmark_throughput_mysql", "MySQL insert records/s")

bench_sync_rate = Gauge("cdc_benchmark_sync_rate_pct", "Sync rate pct")

bench_redis_p50 = Gauge("cdc_benchmark_redis_p50_ms", "Redis p50 latency ms")
bench_redis_p99 = Gauge("cdc_benchmark_redis_p99_ms", "Redis p99 latency ms")

bench_kafka_partitions = Gauge("cdc_benchmark_kafka_partitions", "Kafka partitions")
bench_spark_workers = Gauge("cdc_benchmark_spark_workers", "Spark workers")

bench_trigger_interval = Gauge("cdc_benchmark_trigger_interval_s", "Spark trigger interval")

bench_scale_baseline = Gauge("cdc_scale_baseline_tps", "Throughput baseline")
bench_scale_scaled = Gauge("cdc_scale_scaled_tps", "Throughput scaled")
bench_scale_improvement = Gauge("cdc_scale_improvement_pct", "Scaling improvement")

bench_scale_base_p95 = Gauge("cdc_scale_baseline_p95", "Latency p95 baseline")
bench_scale_scl_p95 = Gauge("cdc_scale_scaled_p95", "Latency p95 scaled")


# =============================
# Real-time rate metrics
# (delta / elapsed giữa 2 lần poll)
# =============================

mysql_insert_rate = Gauge(
    "cdc_mysql_insert_rate",
    "MySQL customer insert rate (records/s)"
)

mongo_write_rate = Gauge(
    "cdc_mongo_write_rate",
    "MongoDB customer write rate (records/s)"
)

kafka_rate_total = Gauge(
    "cdc_kafka_rate_total",
    "Kafka CDC event rate across all topics (events/s)"
)

lag_total = Gauge(
    "cdc_lag_total",
    "Record lag: MySQL customers - MongoDB customers"
)

spark_batch_duration_ms = Gauge(
    "cdc_spark_batch_duration_ms",
    "Spark batch duration ms (triggerExecution from StreamingQueryListener via Redis)"
)

spark_executor_cores = Gauge(
    "cdc_spark_executor_cores",
    "Spark executor cores currently in use (from Spark Master REST API)"
)

spark_executor_memory_mb = Gauge(
    "cdc_spark_executor_memory_mb",
    "Spark executor memory MB currently in use (from Spark Master REST API)"
)


# =============================
# Rate tracking state
# =============================

_rate_state = {
    "ts":          0.0,
    "mysql_c":     0,
    "mongo_c":     0,
    "kafka_total": 0,
}



# =============================
# Collect functions
# =============================


def collect_mysql():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM customers")
        c_count = cur.fetchone()[0]
        mysql_customers.set(c_count)

        cur.execute("SELECT COUNT(*) FROM orders")
        o_count = cur.fetchone()[0]
        mysql_orders.set(o_count)

        conn.close()
        return True, c_count, o_count

    except Exception as e:
        print(f"[MySQL ERROR] {e}")
        return False, 0, 0


def collect_mongo():
    try:
        db = pymongo.MongoClient(MONGO_URI)["inventory"]

        mc = db["customers"].count_documents({})
        mo = db["orders"].count_documents({})

        mongo_customers.set(mc)
        mongo_orders.set(mo)

        return mc, mo

    except Exception as e:
        print(f"[Mongo ERROR] {e}")
        return None, None


def collect_redis():
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )

        revenue = float(r.get("orders:revenue") or 0)
        orders = int(r.get("orders:total") or 0)
        custs = int(r.get("customers:total") or 0)

        redis_orders_revenue.set(revenue)
        redis_orders_total.set(orders)
        redis_customers_total.set(custs)

        top = r.zrevrange("top_customers:order_count", 0, 0, withscores=True)

        if top:
            redis_top_customer_score.set(top[0][1])

        batch_dur = r.get("spark:batch_duration_ms")
        if batch_dur is not None:
            spark_batch_duration_ms.set(float(batch_dur))

        return True

    except Exception as e:
        print(f"[Redis ERROR] {e}")
        return False


def collect_kafka():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER
        )

        offsets = {}

        for topic in TOPICS:
            partitions = consumer.partitions_for_topic(topic)

            if partitions:
                for p in partitions:
                    tp = TopicPartition(topic, p)
                    end = consumer.end_offsets([tp])[tp]
                    offsets[topic] = end

        consumer.close()

        cust_offset = offsets.get(TOPICS[0], 0)
        ord_offset  = offsets.get(TOPICS[1], 0)

        kafka_customers_offset.set(cust_offset)
        kafka_orders_offset.set(ord_offset)

        return True, cust_offset + ord_offset

    except Exception as e:
        print(f"[Kafka ERROR] {e}")
        return False, 0


def collect_spark():
    try:
        import urllib.request
        with urllib.request.urlopen(f"{SPARK_MASTER_URL}/json/", timeout=3) as resp:
            data = json.loads(resp.read())
        workers = data.get("workers", [])
        total_cores = sum(w.get("coresused", 0) for w in workers)
        total_mem   = sum(w.get("memoryused", 0) for w in workers)
        spark_executor_cores.set(total_cores)
        spark_executor_memory_mb.set(total_mem)
    except Exception as e:
        print(f"[Spark ERROR] {e}")


def collect_benchmark():

    try:
        # Đọc dữ liệu từ bản báo cáo scalability cũ (nếu có)
        if SCALABILITY_JSON.exists():
            sr = json.loads(SCALABILITY_JSON.read_text())
            lat = sr.get("latency") or {}
            bench_latency_p50.set(lat.get("p50_s") or 0)
            bench_latency_p95.set(lat.get("p95_s") or 0)
            bench_latency_p99.set(lat.get("p99_s") or 0)
            bench_latency_avg.set(lat.get("avg_s") or 0)
            
            tp = sr.get("throughput_baseline") or {}
            bench_throughput_mysql.set(tp.get("mysql_tps") or 0)
            bench_sync_rate.set(tp.get("sync_rate_pct") or 0)
            
            cfg = sr.get("config") or {}
            bench_spark_workers.set(cfg.get("spark_workers") or 0)
            bench_trigger_interval.set(2)

    except Exception as e:
        print(f"[Benchmark ERROR scalability] {e}")

    try:
        # Đọc dữ liệu từ bản báo cáo scaling cũ (nếu có)
        if SCALING_JSON.exists():
            cmp = json.loads(SCALING_JSON.read_text()).get("comparison") or {}
            bench_scale_baseline.set(cmp.get("baseline_tps") or 0)
            bench_scale_scaled.set(cmp.get("scaled_tps") or 0)
            bench_scale_improvement.set(cmp.get("throughput_improvement_pct") or 0)
            bench_scale_base_p95.set(cmp.get("baseline_p95_s") or 0)
            bench_scale_scl_p95.set(cmp.get("scaled_p95_s") or 0)
    except Exception as e:
        print(f"[Scaling ERROR] {e}")

    try:
        # Đọc dữ liệu từ run_benchmark_v4.py
        if LATEST_BENCHMARK_JSON.exists():
            data = json.loads(LATEST_BENCHMARK_JSON.read_text())
            summary = data.get("summary") or {}
            max_e2e_tps = summary.get("max_e2e_tps") or 0
            bench_throughput_e2e.set(max_e2e_tps)
            
            sus = data.get("sustained") or {}
            if "spark_batch_avg_ms" in sus:
                bench_latency_avg.set(sus["spark_batch_avg_ms"] / 1000.0)
            if "spark_batch_p95_ms" in sus:
                bench_latency_p95.set(sus["spark_batch_p95_ms"] / 1000.0)
                
            cfg = data.get("config") or {}
            bench_kafka_partitions.set(cfg.get("kafka_partitions") or 0)
    except Exception as e:
        print(f"[Latest Benchmark ERROR] {e}")

    try:
        # Đọc dữ liệu từ tps_benchmark.py
        if TPS_RESULTS_JSON.exists():
            data = json.loads(TPS_RESULTS_JSON.read_text())
            results = data.get("results") or []
            if results:
                best = max(results, key=lambda x: x.get("e2e_tps", 0))
                # Nếu latest_benchmark chưa set e2e_tps, dùng của tps_benchmark
                if not LATEST_BENCHMARK_JSON.exists():
                    bench_throughput_e2e.set(best.get("e2e_tps", 0))
                bench_throughput_mysql.set(best.get("mysql_tps", 0))
    except Exception as e:
        print(f"[TPS Benchmark ERROR] {e}")


def check_pipeline_health(mysql_ok, mysql_mc, mysql_mo, mongo_mc, mongo_mo):

    try:

        if mysql_ok and mongo_mc is not None:
            pipeline_up.set(1)
        else:
            pipeline_up.set(0)

        if mongo_mc is not None and mysql_mc is not None:
            in_sync = 1 if (mysql_mc == mongo_mc and mysql_mo == mongo_mo) else 0
            mysql_mongo_in_sync.set(in_sync)

    except Exception as e:

        print(f"[Health ERROR] {e}")
        pipeline_up.set(0)


# =============================
# Main loop
# =============================

if __name__ == "__main__":

    print(f"[{datetime.now()}] Starting metrics exporter on port 8000...")

    start_http_server(8000)

    print(
        f"[{datetime.now()}] Prometheus metrics available at http://localhost:8000/metrics"
    )

    while True:

        try:

            mysql_ok, cur_mysql_c, cur_mysql_o = collect_mysql()

            mongo_mc, mongo_mo = collect_mongo()

            collect_redis()

            kafka_ok, cur_kafka_total = collect_kafka()

            collect_spark()

            collect_benchmark()

            check_pipeline_health(mysql_ok, cur_mysql_c, cur_mysql_o, mongo_mc, mongo_mo)

            # ── Tính rate metrics ──────────────────────────────
            now = time.time()

            if _rate_state["ts"] > 0:
                elapsed = now - _rate_state["ts"]
                if elapsed > 0:
                    cur_mongo_c = mongo_mc if mongo_mc is not None else 0

                    mysql_insert_rate.set(max(0.0, (cur_mysql_c - _rate_state["mysql_c"]) / elapsed))
                    mongo_write_rate.set(max(0.0, (cur_mongo_c - _rate_state["mongo_c"]) / elapsed))
                    kafka_rate_total.set(max(0.0, (cur_kafka_total - _rate_state["kafka_total"]) / elapsed))
                    lag_total.set(max(0, cur_mysql_c - (mongo_mc if mongo_mc is not None else 0)))

            _rate_state["ts"]          = now
            _rate_state["mysql_c"]     = cur_mysql_c
            _rate_state["mongo_c"]     = mongo_mc if mongo_mc is not None else 0
            _rate_state["kafka_total"] = cur_kafka_total
            # ───────────────────────────────────────────────────

            print(f"[{datetime.now()}] Metrics collected OK")

        except Exception as e:

            print(f"[ERROR] {e}")

        time.sleep(5)