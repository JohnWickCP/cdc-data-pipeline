#!/usr/bin/env python3
"""Phase 3 inject: 2500 rec/s x 30s"""
import mysql.connector
import time
import sys

conn = mysql.connector.connect(
    host="127.0.0.1", port=3306,
    user="root", password="root", database="inventory",
    autocommit=True
)
cur = conn.cursor()
rate = 2500
duration = 30
interval = 1.0 / rate
base_id = 1_000_000
i = 0
start = time.time()
while time.time() - start < duration:
    cur.execute(
        "INSERT INTO customers (id, name, email, phone) VALUES (%s, %s, %s, %s)",
        (base_id + i, f"P3_{i}", f"p3_{i}@test.com", "0999000000")
    )
    i += 1
    elapsed = time.time() - start
    drift = (i * interval) - elapsed
    if drift > 0.001:
        time.sleep(drift)
    if i % 5000 == 0:
        print(f"t={elapsed:.0f}s injected={i} rate={i/elapsed:.0f}/s", flush=True)

actual_rate = i / (time.time() - start)
print(f"DONE: {i} records, rate={actual_rate:.1f}/s", flush=True)
cur.close()
conn.close()
