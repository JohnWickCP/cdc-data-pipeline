"""
CDC Pipeline Test Suite
=======================
Tests: Accuracy, Latency, Throughput, Kafka Lag, Fault Tolerance
Kết quả ghi vào: test_report.txt
"""

import time
import pymysql
import pymongo
import redis
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from datetime import datetime
import subprocess

# =============================
# Config kết nối
# =============================
MYSQL_CONFIG    = dict(host="localhost", port=3306, user="root", password="root", db="inventory")
MONGO_URI       = "mongodb://localhost:27017"
REDIS_HOST      = "localhost"
REDIS_PORT      = 6379
KAFKA_BROKER    = "localhost:9092"
TOPICS          = ["inventory.inventory.customers", "inventory.inventory.orders"]
REPORT_FILE     = "test_report.txt"
LATENCY_TIMEOUT = 15  # giây tối đa chờ data xuất hiện ở MongoDB

report_lines = []

def log(msg):
    print(msg)
    report_lines.append(msg)

def section(title):
    log(f"\n{'='*60}\n  {title}\n{'='*60}")

def save_report():
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"\n📄 Báo cáo đã lưu vào: {REPORT_FILE}")

# =============================
# Kết nối
# =============================
def get_mysql():
    return pymysql.connect(**MYSQL_CONFIG)

def get_mongo():
    return pymongo.MongoClient(MONGO_URI)["inventory"]

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# =============================
# TEST 1: Accuracy
# =============================
def test_accuracy():
    section("TEST 1: DATA ACCURACY (Kiểm tra độ chính xác dữ liệu)")
    passed = 0
    failed = 0

    try:
        # MySQL
        conn = get_mysql()
        cur = conn.cursor()
        cur.execute("SELECT id, name, email FROM customers ORDER BY id")
        mysql_customers = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM orders")
        mysql_orders_count = cur.fetchone()[0]
        conn.close()

        # MongoDB
        db = get_mongo()
        mongo_customers_count = db["customers"].count_documents({})
        mongo_orders_count    = db["orders"].count_documents({})

        # Redis
        r = get_redis()

        log(f"\n  [MySQL]   customers={len(mysql_customers)}, orders={mysql_orders_count}")
        log(f"  [MongoDB] customers={mongo_customers_count}, orders={mongo_orders_count}")

        # --- So sánh MySQL ↔ MongoDB (snapshot chính xác) ---
        if len(mysql_customers) == mongo_customers_count:
            log(f"\n  ✅ PASS: customers count khớp MySQL↔MongoDB ({len(mysql_customers)})")
            passed += 1
        else:
            log(f"\n  ❌ FAIL: customers KHÔNG khớp MySQL({len(mysql_customers)}) ↔ MongoDB({mongo_customers_count})")
            failed += 1

        if mysql_orders_count == mongo_orders_count:
            log(f"  ✅ PASS: orders count khớp MySQL↔MongoDB ({mysql_orders_count})")
            passed += 1
        else:
            log(f"  ❌ FAIL: orders KHÔNG khớp MySQL({mysql_orders_count}) ↔ MongoDB({mongo_orders_count})")
            failed += 1

        # --- Kiểm tra từng record cụ thể ---
        mismatches = 0
        for (mid, mname, memail) in mysql_customers:
            doc = db["customers"].find_one({"_id": mid})
            if not doc:
                log(f"  ❌ FAIL: customer id={mid} có trong MySQL nhưng KHÔNG có trong MongoDB")
                mismatches += 1
            else:
                if doc.get("name") != mname:
                    log(f"  ❌ FAIL: customer id={mid} name không khớp: MySQL='{mname}' MongoDB='{doc.get('name')}'")
                    mismatches += 1

        if mismatches == 0:
            log(f"  ✅ PASS: Tất cả {len(mysql_customers)} customers khớp record-by-record")
            passed += 1
        else:
            failed += 1

        # --- PII Masking ---
        sample = db["customers"].find_one({})
        if sample and "email" in sample:
            email = sample["email"]
            if "*" in email:
                log(f"  ✅ PASS: PII Masking hoạt động — email mẫu: {email}")
                passed += 1
            else:
                log(f"  ❌ FAIL: PII Masking KHÔNG hoạt động — email: {email}")
                failed += 1

        # --- Redis string keys ---
        string_keys = ["customers:total", "orders:total", "orders:revenue"]
        for key in string_keys:
            val = r.get(key)
            key_type = r.type(key)
            if val is not None:
                log(f"  ✅ PASS: Redis [{key}] = {val} (type={key_type})")
                passed += 1
            else:
                log(f"  ❌ FAIL: Redis [{key}] không tồn tại")
                failed += 1

        # --- Redis zset keys ---
        zset_keys = ["top_customers:order_count"]
        for key in zset_keys:
            key_type = r.type(key)
            zcard = r.zcard(key)
            if key_type == "zset" and zcard > 0:
                top = r.zrevrange(key, 0, 2, withscores=True)
                log(f"  ✅ PASS: Redis [{key}] = zset với {zcard} entries")
                log(f"           Top 3: {top}")
                passed += 1
            else:
                log(f"  ❌ FAIL: Redis [{key}] không tồn tại hoặc sai kiểu (type={key_type})")
                failed += 1

        log(f"\n  ℹ️  NOTE: Redis counters là metrics tích lũy (mỗi event đều INCR)")
        log(f"           Không so sánh trực tiếp với MySQL snapshot — đây là thiết kế đúng.")

    except Exception as e:
        log(f"  ❌ ERROR: {e}")
        failed += 1

    log(f"\n  KẾT QUẢ: {passed} passed, {failed} failed")
    return passed, failed

# =============================
# TEST 2: End-to-End Latency
# =============================
def test_latency():
    section("TEST 2: END-TO-END LATENCY (Độ trễ đầu cuối)")

    latency = -1
    inserted_id = None
    try:
        db = get_mongo()
        conn = get_mysql()
        cur = conn.cursor()

        test_email = f"latency_test_{int(time.time())}@test.com"
        cur.execute("INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
                    ("Latency Test User", test_email, "0000000000"))
        conn.commit()
        inserted_id = cur.lastrowid
        conn.close()

        t_insert = time.time()
        log(f"\n  → INSERT customer id={inserted_id} lúc {datetime.now().strftime('%H:%M:%S')}")
        log(f"  → Đang chờ xuất hiện ở MongoDB (timeout={LATENCY_TIMEOUT}s)...")

        found = False
        while time.time() - t_insert < LATENCY_TIMEOUT:
            doc = db["customers"].find_one({"_id": inserted_id})
            if doc:
                latency = time.time() - t_insert
                found = True
                break
            time.sleep(0.5)

        if found:
            log(f"  ✅ PASS: Data xuất hiện ở MongoDB sau {latency:.2f}s")
            if latency < 10:
                log(f"  ✅ PASS: Latency {latency:.2f}s < 10s (đạt mục tiêu đề tài)")
            else:
                log(f"  ⚠️  WARN: Latency {latency:.2f}s >= 10s (vượt mục tiêu)")
        else:
            log(f"  ❌ FAIL: Không thấy data ở MongoDB sau {LATENCY_TIMEOUT}s")

    except Exception as e:
        log(f"  ❌ ERROR: {e}")
    finally:
        if inserted_id:
            try:
                conn2 = get_mysql()
                cur2 = conn2.cursor()
                cur2.execute("DELETE FROM customers WHERE id = %s", (inserted_id,))
                conn2.commit()
                conn2.close()
            except:
                pass

    return latency

# =============================
# TEST 3: Throughput
# =============================
def test_throughput():
    section("TEST 3: THROUGHPUT (Số events/giây)")

    N = 50
    inserted_ids = []
    throughput = -1
    try:
        conn = get_mysql()
        cur = conn.cursor()
        db = get_mongo()

        before = db["customers"].count_documents({})

        log(f"\n  → Inserting {N} customers vào MySQL...")
        t_start = time.time()
        for i in range(N):
            cur.execute("INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
                        (f"Throughput User {i}", f"tp_{i}_{int(time.time())}@test.com", "0111222333"))
            inserted_ids.append(cur.lastrowid)
        conn.commit()
        conn.close()
        insert_time = time.time() - t_start
        log(f"  → MySQL insert {N} records xong sau {insert_time:.2f}s")
        log(f"  → Đang chờ pipeline xử lý (tối đa 30s)...")

        deadline = time.time() + 30
        while time.time() < deadline:
            after = db["customers"].count_documents({})
            if after >= before + N:
                break
            time.sleep(1)

        total_time = time.time() - t_start
        after = db["customers"].count_documents({})
        synced = after - before
        throughput = synced / total_time if total_time > 0 else 0

        log(f"  → Synced {synced}/{N} records trong {total_time:.2f}s")
        log(f"  ✅ Throughput: {throughput:.2f} records/giây")
        if synced < N:
            log(f"  ⚠️  WARN: {N - synced} records chưa sync (có thể cần thêm thời gian)")

    except Exception as e:
        log(f"  ❌ ERROR: {e}")
    finally:
        if inserted_ids:
            try:
                conn3 = get_mysql()
                cur3 = conn3.cursor()
                cur3.executemany("DELETE FROM customers WHERE id = %s", [(i,) for i in inserted_ids])
                conn3.commit()
                conn3.close()
            except:
                pass

    return throughput

# =============================
# TEST 4: Kafka Lag
# =============================
def test_kafka_lag():
    section("TEST 4: KAFKA LAG (Tồn đọng message)")

    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)

        log("")
        total_messages = 0
        for topic in TOPICS:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                log(f"  ⚠️  Topic {topic} không có partition")
                continue
            for p in partitions:
                tp = TopicPartition(topic, p)
                beginning = consumer.beginning_offsets([tp])[tp]
                end = consumer.end_offsets([tp])[tp]
                total = end - beginning
                total_messages += total
                log(f"  Topic: {topic}")
                log(f"    Partition {p}: offset {beginning} → {end} | Tổng messages: {total}")

        log(f"\n  Tổng messages trong tất cả topics: {total_messages}")

        # Consumer groups
        result = subprocess.run(
            ["docker", "exec", "cdc-kafka", "kafka-consumer-groups",
             "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True
        )
        groups = [g.strip() for g in result.stdout.strip().split("\n") if g.strip()]

        if groups:
            log(f"\n  Consumer groups: {groups}")
            for group in groups:
                lag_result = subprocess.run(
                    ["docker", "exec", "cdc-kafka", "kafka-consumer-groups",
                     "--bootstrap-server", "localhost:9092",
                     "--describe", "--group", group],
                    capture_output=True, text=True
                )
                if lag_result.stdout.strip():
                    log(f"\n  --- Group: {group} ---")
                    for line in lag_result.stdout.strip().split("\n"):
                        log(f"  {line}")
        else:
            log(f"\n  ℹ️  Không có consumer group nào — Spark dùng internal offset tracking")

        consumer.close()
        log(f"\n  ✅ Kafka lag check hoàn tất")

    except Exception as e:
        log(f"  ❌ ERROR: {e}")

# =============================
# TEST 5: Fault Tolerance
# =============================
def test_fault_tolerance():
    section("TEST 5: FAULT TOLERANCE (Khả năng chịu lỗi)")

    inserted_id = None
    try:
        db = get_mongo()
        before = db["customers"].count_documents({})
        log(f"\n  → MongoDB customers trước restart: {before}")

        log("  → Restart cdc-spark-master...")
        subprocess.run(["docker", "restart", "cdc-spark-master"], capture_output=True)
        time.sleep(15)

        log("  → Khởi động lại Spark job...")
        subprocess.run([
            "docker", "exec", "-d", "cdc-spark-master", "/bin/bash", "-c",
            "/opt/spark/bin/spark-submit --class CdcRedisConsumer "
            "--master spark://cdc-spark-master:7077 "
            "/opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar "
            "> /tmp/spark_metrics.log 2>&1"
        ])
        time.sleep(20)

        conn = get_mysql()
        cur = conn.cursor()
        cur.execute("INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
                    ("Fault Tolerance Test", f"fault_{int(time.time())}@test.com", "0999000111"))
        conn.commit()
        inserted_id = cur.lastrowid
        conn.close()

        log(f"  → INSERT customer id={inserted_id} sau restart")
        log("  → Chờ pipeline phục hồi (tối đa 30s)...")

        t_start = time.time()
        found = False
        while time.time() - t_start < 30:
            doc = db["customers"].find_one({"_id": inserted_id})
            if doc:
                found = True
                recovery_time = time.time() - t_start
                break
            time.sleep(1)

        if found:
            log(f"  ✅ PASS: Pipeline phục hồi sau restart, xử lý data sau {recovery_time:.1f}s")
        else:
            log(f"  ❌ FAIL: Pipeline KHÔNG phục hồi trong 30s")

    except Exception as e:
        log(f"  ❌ ERROR: {e}")
    finally:
        if inserted_id:
            try:
                conn2 = get_mysql()
                cur2 = conn2.cursor()
                cur2.execute("DELETE FROM customers WHERE id = %s", (inserted_id,))
                conn2.commit()
                conn2.close()
            except:
                pass

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    start_time = datetime.now()
    log(f"CDC PIPELINE TEST REPORT")
    log(f"Thời gian chạy: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"{'='*60}")

    acc_passed, acc_failed = test_accuracy()
    latency    = test_latency()
    throughput = test_throughput()
    test_kafka_lag()
    test_fault_tolerance()

    section("TỔNG KẾT")
    log(f"  Accuracy   : {acc_passed} passed, {acc_failed} failed")
    log(f"  Latency    : {latency:.2f}s" if latency >= 0 else "  Latency    : ERROR")
    log(f"  Throughput : {throughput:.2f} records/s" if throughput >= 0 else "  Throughput : ERROR")
    log(f"\n  Thời gian hoàn thành: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    save_report()