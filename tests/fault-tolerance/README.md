# Fault Tolerance Testing — CDC Pipeline

Hướng dẫn test khả năng chịu lỗi trên môi trường VM.

---

## Setup nhanh (laptop hiện tại)

```bash
# Chạy từ root của project
bash tests/fault-tolerance/run_ft_tests.sh all      # test cả 3 scenarios
bash tests/fault-tolerance/run_ft_tests.sh kafka    # chỉ Kafka crash
bash tests/fault-tolerance/run_ft_tests.sh debezium # chỉ Debezium restart
bash tests/fault-tolerance/run_ft_tests.sh spark    # chỉ Spark kill
```

Hoặc dùng dashboard: http://localhost:8888 → tab **Fault Tolerance**

---

## 3 Scenarios (Single-broker — laptop)

| Scenario | Action | Expected | Auto-recover |
|---|---|---|---|
| Kafka Crash | `docker stop cdc-kafka` + restart | 0 data lost, ~45s | ✅ |
| Debezium Restart | `docker restart cdc-debezium` | 0 event lost, ~30s | ✅ |
| Spark Kill | `pkill CdcRedisConsumer` + resubmit | 0 duplicate, ~90s | ⚠ Manual |

---

## Scenario 4: Kafka Multi-broker (VM only)

**Yêu cầu VM:** >= 8 core, >= 16 GB RAM

### Khởi động 3-broker cluster

```bash
# Thay thế single-broker bằng 3-broker cluster
docker compose -f docker-compose.yml -f tests/fault-tolerance/docker-compose.kafka-cluster.yml up -d

# Verify 3 brokers running
docker ps | grep kafka
# Expected: cdc-kafka, cdc-kafka-2, cdc-kafka-3

# Test ISR failover
bash tests/fault-tolerance/run_ft_tests.sh cluster
```

### Kết quả mong đợi (3-broker cluster)

```
SCENARIO 4: Kafka Multi-broker ISR Failover
[INFO] 3-broker cluster detected: cdc-kafka, cdc-kafka-2, cdc-kafka-3
[INFO] Killing ONE broker (cdc-kafka-2) — minority failure
[INFO] Pipeline should continue WITHOUT interruption
[PASS] Cluster Failover — PASS | Recovery: 2s | ISR absorbed the failure
```

So sánh với single-broker:
- Single broker crash: pipeline dừng ~45s, data stuck in Kafka buffer
- 3-broker cluster: pipeline KHÔNG dừng, recovery < 5s (ISR failover)

---

## Đọc kết quả

```bash
cat tests/fault-tolerance/ft_results.jsonl | python3 -c "
import json, sys
for line in sys.stdin:
    r = json.loads(line)
    status = '✅' if r['status'] == 'PASS' else '⚠️'
    print(f\"{status} {r['scenario']:30s} | {r['status']:8s} | {r['recovery_s']:3d}s | lost={r['lost']} | ts={r['ts']}\")"
```

---

## Giải thích tại sao test này ý nghĩa

### Single broker (laptop/demo)
- Kafka crash → data **không mất** vì Debezium buffer binlog offset
- Nhưng pipeline **dừng** trong thời gian crash (~45s outage)
- → Production không chấp nhận được

### 3-broker cluster (VM)
- Kafka crash 1 broker → `replication.factor=3`, `min.insync.replicas=2`
- 2 broker còn lại đủ quorum → pipeline **không dừng**
- ISR (In-Sync Replica) failover < 5s
- → Production-grade fault tolerance

**Message cho hội đồng:** "Hệ thống này đã thiết kế cho production readiness. Với setup hiện tại (1 broker), recovery là 45s — không mất data. Với 3-broker cluster trên VM, recovery < 5s — pipeline không bao giờ dừng. Con số này đo được và có thể reproduce."
