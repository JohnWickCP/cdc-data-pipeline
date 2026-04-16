#!/usr/bin/env python3

"""
metrics_exporter.py
Thu thập metrics từ MySQL, MongoDB, Redis, Kafka
và benchmark JSON để Prometheus đọc.
"""

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
    host="localhost",
    port=3306,
    user="root",
    password="root",
    db="inventory"
)

MONGO_URI = "mongodb://localhost:27017"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

KAFKA_BROKER = "localhost:9092"

TOPICS = [
    "inventory.inventory.customers",
    "inventory.inventory.orders"
]


# =============================
# Benchmark JSON
# =============================

SCALABILITY_JSON = Path(__file__).parent / "benchmark" / "results" / "scalability_report.json"
SCALING_JSON = Path(__file__).parent / "benchmark" / "results" / "scaling_results.json"


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
# Collect functions
# =============================


def collect_mysql():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM customers")
        mysql_customers.set(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM orders")
        mysql_orders.set(cur.fetchone()[0])

        conn.close()
        return True

    except Exception as e:
        print(f"[MySQL ERROR] {e}")
        return False


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

        kafka_customers_offset.set(offsets.get(TOPICS[0], 0))
        kafka_orders_offset.set(offsets.get(TOPICS[1], 0))

        return True

    except Exception as e:
        print(f"[Kafka ERROR] {e}")
        return False


def collect_benchmark():

    try:

        if SCALABILITY_JSON.exists():

            sr = json.loads(SCALABILITY_JSON.read_text())

            lat = sr.get("latency") or {}

            bench_latency_p50.set(lat.get("p50_s") or 0)
            bench_latency_p95.set(lat.get("p95_s") or 0)
            bench_latency_p99.set(lat.get("p99_s") or 0)
            bench_latency_avg.set(lat.get("avg_s") or 0)

            tp = sr.get("throughput_baseline") or {}

            bench_throughput_e2e.set(tp.get("e2e_tps") or 0)
            bench_throughput_mysql.set(tp.get("mysql_tps") or 0)
            bench_sync_rate.set(tp.get("sync_rate_pct") or 0)

            rl = sr.get("redis_latency") or {}

            if rl:
                bench_redis_p50.set(max((v.get("p50_ms") or 0) for v in rl.values()))
                bench_redis_p99.set(max((v.get("p99_ms") or 0) for v in rl.values()))

            cfg = sr.get("config") or {}

            bench_kafka_partitions.set(cfg.get("kafka_partitions_orders") or 0)
            bench_spark_workers.set(cfg.get("spark_workers") or 0)

            bench_trigger_interval.set(2)

    except Exception as e:
        print(f"[Benchmark ERROR] {e}")

    try:

        if SCALING_JSON.exists():

            cmp = json.loads(SCALING_JSON.read_text()).get("comparison") or {}

            bench_scale_baseline.set(cmp.get("baseline_tps") or 0)
            bench_scale_scaled.set(cmp.get("scaled_tps") or 0)
            bench_scale_improvement.set(cmp.get("throughput_improvement_pct") or 0)

            bench_scale_base_p95.set(cmp.get("baseline_p95_s") or 0)
            bench_scale_scl_p95.set(cmp.get("scaled_p95_s") or 0)

    except Exception as e:
        print(f"[Scaling ERROR] {e}")


def check_pipeline_health(mysql_ok, mongo_mc, mongo_mo):

    try:

        if mysql_ok and mongo_mc is not None:
            pipeline_up.set(1)
        else:
            pipeline_up.set(0)

        if mongo_mc is not None:

            conn = pymysql.connect(**MYSQL_CONFIG)
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM customers")
            mc = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM orders")
            mo = cur.fetchone()[0]

            conn.close()

            in_sync = 1 if (mc == mongo_mc and mo == mongo_mo) else 0
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

            mysql_ok = collect_mysql()

            mongo_mc, mongo_mo = collect_mongo()

            collect_redis()

            collect_kafka()

            collect_benchmark()

            check_pipeline_health(mysql_ok, mongo_mc, mongo_mo)

            print(f"[{datetime.now()}] Metrics collected OK")

        except Exception as e:

            print(f"[ERROR] {e}")

        time.sleep(5)