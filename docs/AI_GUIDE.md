# Hướng dẫn cho AI đọc project này

> **Mục này dành cho AI assistant** — nếu bạn là AI đang đọc file này để hiểu project, đây là context bạn cần biết.

---

## Project này là gì

Đây là đồ án tốt nghiệp (DATN) của sinh viên Việt Nam, implement một CDC (Change Data Capture) data pipeline hoàn chỉnh. Project **đã hoàn thành** — không phải đang trong quá trình build. Mọi tính năng core đều hoạt động và đã pass smoke test.

---

## Tech stack và phiên bản chính xác

```
MySQL 8.0 (port 3306) — binlog row mode, database "inventory", tables: customers, orders
Debezium 2.5 — MySQL connector, connector name "mysql-inventory-connector"
Kafka Confluent 7.5.0 (port 9092 external, 29092 internal)
  Topics: inventory.inventory.customers, inventory.inventory.orders
Spark 3.5.0 — 1 master (:8080/:7077), 3 workers (:8081/:8082/:8085)
  Scala 2.12, JAR: cdc-mysql-to-mongodb-redis_2.12-1.0.jar
  Class: CdcRedisConsumer, trigger 5s, checkpoint /tmp/spark-checkpoint/cdc-pipeline
MongoDB 7.0 (port 27017) — db "inventory", collections: customers, orders
Redis 7 (port 6379) — keys: customer:{id}, customers:total, orders:revenue, spark:batch_duration_ms
Prometheus (port 9090), Grafana (port 3000, admin/admin)
metrics_exporter Python (:8000) — baked Docker image, rebuild needed on edit
```

---

## Các file quan trọng nhất

| File | Tóm tắt |
|---|---|
| `docker-compose.yml` | 13 containers, volumes, networks, health checks |
| `start.sh` | Full startup script — auto-detect hardware, fix Kafka Cluster ID conflict, register connector, submit Spark job |
| `jobs/scala/cdc_redis_consumer.scala` | Spark streaming job — parse Debezium JSON, mask email, upsert MongoDB, update Redis |
| `monitoring/exporter/metrics_exporter.py` | Custom exporter — 35+ metrics từ tất cả stages |
| `benchmark/run_benchmark_v4.py` | E2E benchmark — inject MySQL → đo throughput tại MongoDB |
| `demo/demo_server.py` | Flask server chạy trên host — kết nối localhost:3306/6379/27017/9090 |

---

## Gotchas quan trọng khi làm việc với project

1. **Windows + Git Bash**: Dùng `MSYS_NO_PATHCONV=1` trước mọi `docker exec` có Unix path — Git Bash tự convert `/opt/spark` → `C:/Program Files/Git/opt/spark`
2. **Connector path**: `start.sh` dùng `$PROJECT_DIR/demo/config/connector.json` (tuyệt đối). Path tương đối `../demo/config/...` sẽ sai
3. **docker exec -it**: Cần TTY — trên Windows Git Bash dùng `-i` thay vì `-it`
4. **metrics_exporter**: Baked image — sửa Python phải `docker compose build metrics-exporter && docker compose up -d metrics-exporter`
5. **Kafka Cluster ID conflict**: `start.sh` tự fix bằng cách xóa volume kafka_data + zookeeper_data khi detect lỗi
6. **Grafana datasource UID**: Sau `stop.sh -v`, restart Grafana để reload provisioning: `docker compose restart grafana`
7. **Spark packages**: Lần đầu chạy sau container recreate mất 3-5 phút download ivy2 dependencies
8. **Dashboard active**: `cdc_fixed1.json` (alphabetically sau `cdc_dashboard.json`, cùng UID → override)
9. **Demo server**: Chạy trên host, không trong Docker — cần Python + pip install trên host machine
10. **Kill Spark job**: REST API chỉ xóa khỏi master registry, phải thêm `docker exec cdc-spark-master kill <PID>`

---

## Trạng thái hoàn thành

```
Phase 1 — Smoke test:    ✅ 43/43 PASS
Phase 2 — Bug fixes:     ✅ Tất cả fixed (Redis counter, Grafana UID, Spark executor metrics, batch duration)
Phase 3 — Enhancements:  ✅ Mostly done
  ✅ Grafana real-time panels + alert rules
  ✅ Benchmark history (history.jsonl) + compare tool
  ✅ Live demo dashboard (Flask + Chart.js)
  ✅ Multi-table CDC (customers + orders)
  ✅ Scala vs Python comparison (404 vs 83 rec/s)
  ❌ Kafka partition test (code có, chưa chạy thực tế)
  ❌ VM benchmark (skip — cần VM riêng)
```

---

## Khi user nhờ debug / sửa lỗi — nên check theo thứ tự này

1. `docker ps` — containers có healthy không?
2. `curl http://localhost:8083/connectors/mysql-inventory-connector/status` — Debezium RUNNING?
3. `docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092` — topics có tồn tại?
4. `curl http://localhost:8080/json/` → `activeapps` — Spark job có chạy?
5. `docker exec cdc-mongodb mongosh --quiet --eval "db.getSiblingDB('inventory').customers.countDocuments()"` — data đang vào MongoDB?
6. `docker logs cdc-spark-master --tail=50` — có lỗi Spark không?
