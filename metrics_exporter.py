"""
metrics_exporter.py
===================
Thu thập metrics từ Redis, MongoDB, Kafka và expose ra cổng 8000
để Prometheus đọc mỗi 5 giây.

Chạy: python3 metrics_exporter.py
"""

import time
import pymysql
import pymongo
import redis
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from prometheus_client import start_http_server, Gauge
from datetime import datetime

# =============================
# Config
# =============================
MYSQL_CONFIG = dict(host="localhost", port=3306, user="root", password="root", db="inventory")
MONGO_URI    = "mongodb://localhost:27017"
REDIS_HOST   = "localhost"
REDIS_PORT   = 6379
KAFKA_BROKER = "localhost:9092"
TOPICS       = ["inventory.inventory.customers", "inventory.inventory.orders"]

# =============================
# Prometheus Metrics
# =============================
# MySQL
mysql_customers   = Gauge("cdc_mysql_customers_total",   "Số customers trong MySQL")
mysql_orders      = Gauge("cdc_mysql_orders_total",      "Số orders trong MySQL")

# MongoDB
mongo_customers   = Gauge("cdc_mongo_customers_total",   "Số customers trong MongoDB")
mongo_orders      = Gauge("cdc_mongo_orders_total",      "Số orders trong MongoDB")

# Redis
redis_orders_revenue     = Gauge("cdc_redis_orders_revenue",      "Tổng doanh thu trong Redis")
redis_orders_total       = Gauge("cdc_redis_orders_total",        "Tổng số orders trong Redis")
redis_customers_total    = Gauge("cdc_redis_customers_total",     "Tổng events customers trong Redis")
redis_top_customer_score = Gauge("cdc_redis_top_customer_score",  "Số orders của top customer")

# Kafka
kafka_customers_offset   = Gauge("cdc_kafka_customers_offset",    "Kafka offset topic customers")
kafka_orders_offset      = Gauge("cdc_kafka_orders_offset",       "Kafka offset topic orders")

# Pipeline health
pipeline_up              = Gauge("cdc_pipeline_up",               "Pipeline đang chạy (1=up, 0=down)")
mysql_mongo_in_sync      = Gauge("cdc_mysql_mongo_in_sync",       "MySQL và MongoDB có khớp không (1=khớp)")

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
        print(f"[MongoDB ERROR] {e}")
        return None, None

def collect_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        revenue = float(r.get("orders:revenue") or 0)
        orders  = int(r.get("orders:total") or 0)
        custs   = int(r.get("customers:total") or 0)

        redis_orders_revenue.set(revenue)
        redis_orders_total.set(orders)
        redis_customers_total.set(custs)

        # Top customer score
        top = r.zrevrange("top_customers:order_count", 0, 0, withscores=True)
        if top:
            redis_top_customer_score.set(top[0][1])
        return True
    except Exception as e:
        print(f"[Redis ERROR] {e}")
        return False

def collect_kafka():
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
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

def check_pipeline_health(mysql_ok, mongo_mc, mongo_mo):
    try:
        # Pipeline up nếu kết nối được tất cả
        if mysql_ok and mongo_mc is not None:
            pipeline_up.set(1)
        else:
            pipeline_up.set(0)

        # Sync check: MySQL customers == MongoDB customers
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
    print(f"[{datetime.now()}] Prometheus metrics available at http://localhost:8000/metrics")

    while True:
        try:
            mysql_ok        = collect_mysql()
            mongo_mc, mongo_mo = collect_mongo()
            collect_redis()
            collect_kafka()
            check_pipeline_health(mysql_ok, mongo_mc, mongo_mo)
            print(f"[{datetime.now()}] Metrics collected OK")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(5)