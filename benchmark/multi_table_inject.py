"""
Multi-table concurrent injection: customers + orders.
Chạy từ host (python benchmark/multi_table_inject.py).
Schema orders thực tế: id, customer_id (FK→customers.id), order_date, total_amount, status.
"""
import threading
import time
import random
import sys
import pymysql

MYSQL_CONFIG = dict(
    host='127.0.0.1', port=3306,
    user='root', password='root',
    database='inventory', autocommit=True
)

BASE_CUST = 2_000_000
BASE_ORD  = 5_000_000

EXISTING_CUSTOMER_IDS = [1, 2, 3]  # FK reference

ORDER_STATUSES = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']


BATCH_SIZE = 10  # batch INSERT để đạt tốc độ cao


def inject_customers(rate, duration, results):
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    batch_interval = BATCH_SIZE / rate  # seconds per batch
    i = 0
    start = time.time()
    try:
        while time.time() - start < duration:
            batch_start = i
            vals = []
            for b in range(BATCH_SIZE):
                vals.append((BASE_CUST + i + b, f"MT_C{i+b}", f"mtc{i+b}@test.com", "0911111111"))
            cur.executemany(
                "INSERT INTO customers (id, name, email, phone) VALUES (%s, %s, %s, %s)",
                vals
            )
            i += BATCH_SIZE
            drift = (i / rate) - (time.time() - start)
            if drift > 0.001:
                time.sleep(drift)
    except Exception as e:
        print(f"[customers] ERROR at i={i}: {e}")
    elapsed = time.time() - start
    results['customers'] = {'count': i, 'elapsed': elapsed, 'rate': i / elapsed if elapsed > 0 else 0}
    print(f"[customers] {i} records in {elapsed:.1f}s = {i/elapsed:.1f} rec/s")
    conn.close()


def inject_orders(rate, duration, results):
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    i = 0
    start = time.time()
    import datetime
    try:
        while time.time() - start < duration:
            vals = []
            now_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for b in range(BATCH_SIZE):
                cust_id = random.choice(EXISTING_CUSTOMER_IDS)
                status = random.choice(ORDER_STATUSES)
                amount = round(random.uniform(10.0, 500.0), 2)
                vals.append((BASE_ORD + i + b, cust_id, now_ts, amount, status))
            cur.executemany(
                "INSERT INTO orders (id, customer_id, order_date, total_amount, status) "
                "VALUES (%s, %s, %s, %s, %s)",
                vals
            )
            i += BATCH_SIZE
            drift = (i / rate) - (time.time() - start)
            if drift > 0.001:
                time.sleep(drift)
    except Exception as e:
        print(f"[orders] ERROR at i={i}: {e}")
    elapsed = time.time() - start
    results['orders'] = {'count': i, 'elapsed': elapsed, 'rate': i / elapsed if elapsed > 0 else 0}
    print(f"[orders] {i} records in {elapsed:.1f}s = {i/elapsed:.1f} rec/s")
    conn.close()


def monitor_loop(duration, stop_event):
    """Print lag + sync status mỗi 15s trong khi inject."""
    start = time.time()
    sample_n = 0
    while not stop_event.is_set() and time.time() - start < duration + 30:
        time.sleep(15)
        if stop_event.is_set():
            break
        sample_n += 1
        try:
            import subprocess
            ts = time.strftime('%H:%M:%S')

            cust_my = subprocess.run(
                ['docker', 'exec', 'cdc-mysql', 'mysql', '-uroot', '-proot', 'inventory',
                 '-Nse', f'SELECT COUNT(*) FROM customers WHERE id >= {BASE_CUST}'],
                capture_output=True, text=True, timeout=5
            ).stdout.strip()

            ord_my = subprocess.run(
                ['docker', 'exec', 'cdc-mysql', 'mysql', '-uroot', '-proot', 'inventory',
                 '-Nse', f'SELECT COUNT(*) FROM orders WHERE id >= {BASE_ORD}'],
                capture_output=True, text=True, timeout=5
            ).stdout.strip()

            cust_mg = subprocess.run(
                ['docker', 'exec', 'cdc-mongodb', 'mongosh', 'inventory', '--quiet',
                 '--eval', f'db.customers.countDocuments({{_id:{{$gte:{BASE_CUST}}}}})', ],
                capture_output=True, text=True, timeout=5
            ).stdout.strip().splitlines()[-1] if True else '?'

            ord_mg = subprocess.run(
                ['docker', 'exec', 'cdc-mongodb', 'mongosh', 'inventory', '--quiet',
                 '--eval', f'db.orders.countDocuments({{_id:{{$gte:{BASE_ORD}}}}})', ],
                capture_output=True, text=True, timeout=5
            ).stdout.strip().splitlines()[-1] if True else '?'

            lag = subprocess.run(
                ['curl', '-s', 'http://localhost:8000/metrics'],
                capture_output=True, text=True, timeout=3
            )
            lag_val = '?'
            for line in lag.stdout.splitlines():
                if line.startswith('cdc_lag_total '):
                    lag_val = line.split()[1]

            print(f"  [{ts}] cust: mysql={cust_my} mongo={cust_mg} | "
                  f"ord: mysql={ord_my} mongo={ord_mg} | lag={lag_val}")
        except Exception as e:
            print(f"  [monitor] error: {e}")


def cleanup():
    """Xóa test data sau khi chạy xong."""
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    cur.execute(f"DELETE FROM orders WHERE id >= {BASE_ORD}")
    cur.execute(f"DELETE FROM customers WHERE id >= {BASE_CUST}")
    conn.close()
    print(f"Cleanup: deleted orders id>={BASE_ORD} and customers id>={BASE_CUST}")


def run(cust_rate, ord_rate, duration):
    print(f"\n{'='*60}")
    print(f"Multi-table inject: {cust_rate} cust/s + {ord_rate} ord/s × {duration}s")
    print(f"Total target: {cust_rate + ord_rate} events/s")
    print(f"{'='*60}")

    results = {}
    stop_monitor = threading.Event()

    t1 = threading.Thread(target=inject_customers, args=(cust_rate, duration, results))
    t2 = threading.Thread(target=inject_orders,   args=(ord_rate,  duration, results))
    tm = threading.Thread(target=monitor_loop,    args=(duration, stop_monitor), daemon=True)

    t1.start(); t2.start(); tm.start()
    t1.join();  t2.join()
    stop_monitor.set()

    total_injected = results.get('customers', {}).get('count', 0) + \
                     results.get('orders', {}).get('count', 0)
    total_elapsed  = max(results.get('customers', {}).get('elapsed', 1),
                         results.get('orders', {}).get('elapsed', 1))
    print(f"\nTotal injected: {total_injected} events in {total_elapsed:.1f}s "
          f"= {total_injected/total_elapsed:.1f} events/s")
    return results


if __name__ == '__main__':
    # Parse args: python multi_table_inject.py [rate_per_table] [duration]
    rate = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    dur  = int(sys.argv[2]) if len(sys.argv) > 2 else 60

    run(rate, rate, dur)

    print("\nWaiting 15s for pipeline to drain before measuring final sync...")
    time.sleep(15)

    # Final sync check
    import subprocess
    try:
        cust_my = subprocess.run(
            ['docker', 'exec', 'cdc-mysql', 'mysql', '-uroot', '-proot', 'inventory',
             '-Nse', f'SELECT COUNT(*) FROM customers WHERE id >= {BASE_CUST}'],
            capture_output=True, text=True, timeout=5
        ).stdout.strip()
        ord_my = subprocess.run(
            ['docker', 'exec', 'cdc-mysql', 'mysql', '-uroot', '-proot', 'inventory',
             '-Nse', f'SELECT COUNT(*) FROM orders WHERE id >= {BASE_ORD}'],
            capture_output=True, text=True, timeout=5
        ).stdout.strip()
        cust_mg = subprocess.run(
            ['docker', 'exec', 'cdc-mongodb', 'mongosh', 'inventory', '--quiet',
             '--eval', f'db.customers.countDocuments({{_id:{{$gte:{BASE_CUST}}}}})', ],
            capture_output=True, text=True, timeout=5
        ).stdout.strip().splitlines()[-1]
        ord_mg = subprocess.run(
            ['docker', 'exec', 'cdc-mongodb', 'mongosh', 'inventory', '--quiet',
             '--eval', f'db.orders.countDocuments({{_id:{{$gte:{BASE_ORD}}}}})', ],
            capture_output=True, text=True, timeout=5
        ).stdout.strip().splitlines()[-1]

        print(f"\nFinal sync check (15s after inject):")
        print(f"  Customers: MySQL={cust_my}  MongoDB={cust_mg}  "
              f"({'SYNCED' if cust_my == cust_mg else 'LAG'})")
        print(f"  Orders:    MySQL={ord_my}   MongoDB={ord_mg}   "
              f"({'SYNCED' if ord_my == ord_mg else 'LAG'})")

        lag = subprocess.run(
            ['curl', '-s', 'http://localhost:8000/metrics'],
            capture_output=True, text=True, timeout=3
        )
        for line in lag.stdout.splitlines():
            if line.startswith('cdc_lag_total '):
                print(f"  Kafka lag: {line.split()[1]}")
    except Exception as e:
        print(f"Final check error: {e}")
