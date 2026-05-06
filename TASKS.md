# TASKS.md — CDC Data Pipeline

Theo dõi công việc: testing, bug fix, cải tiến.

**Workflow:** Test từng chức năng → ghi nhận lỗi → fix theo priority → commit.

---

## Ký hiệu

| Ký hiệu | Ý nghĩa |
|---|---|
| ✅ | Hoàn thành |
| 🔄 | Đang làm |
| ❌ | Chưa làm |
| ⚠️ | Có vấn đề, cần xem xét |

---

## Phase 1 — Smoke Test & Kiểm tra chức năng

Mục tiêu: xác nhận từng tính năng hoạt động đúng trước khi sửa.

| # | Task | Trạng thái | Ghi chú |
|---|---|---|---|
| 1.1 | Pipeline khởi động đầy đủ (12 containers healthy) | ✅ | All up. Fix Python detection stub Windows → dùng `--version` test thay vì `command -v` |
| 1.2 | INSERT event: MySQL → Kafka → MongoDB | ✅ | Email mask đúng: `phamthid@gmail.com` → `p******d@gmail.com` |
| 1.3 | INSERT event: MongoDB → Redis (hash + counter) | ✅ | `customer:4` hash OK, `customers:total` tăng đúng |
| 1.4 | UPDATE event end-to-end | ✅ | MongoDB + Redis cập nhật đúng, email re-mask đúng |
| 1.5 | DELETE event end-to-end | ⚠️ | MongoDB xóa đúng ✅, Redis key xóa đúng ✅, nhưng `customers:total` không giảm (bug) |
| 1.6 | Debezium connector active (status = RUNNING) | ✅ | connector + task đều RUNNING |
| 1.7 | Spark job đang chạy (xuất hiện trên Spark Master UI) | ✅ | 1 app "CDC-MySQL-To-MongoDB-Redis" RUNNING |
| 1.8 | Prometheus scrape thành công | ✅ | target cdc-pipeline health=up |
| 1.9 | Grafana dashboard hiển thị data | ✅ | Xác nhận thủ công — hiển thị OK |
| 1.10 | Metrics exporter trả về metrics | ✅ | 20+ metrics đúng, thấy rõ bug `cdc_redis_customers_total=5` |
| 1.11 | Benchmark quick mode chạy được | ✅ | 100→90.7, 200→181.2, 500→403.6 rec/s E2E. RAM/Spark cores=0 là bugs riêng (2A.3, 2B.3) |
| 1.12 | Smoke test script pass | ✅ | 43/43 PASS. Fix: Python stub, `|| true` fallback, `aliveworkers` là int không phải list |

---

## Phase 2 — Bug Fix

### 2A — Easy (ít rủi ro, 1-dòng hoặc label change)

| # | Bug | Priority | Trạng thái | File |
|---|---|---|---|---|
| 2A.1 | Đổi nhãn "TPS" → "records/s" trong benchmark output | Low | ✅ | `benchmark/run_benchmark_v4.py` |
| 2A.2 | Đồng trigger interval Python: 10s → 5s (bằng Scala) | Medium | ✅ | Code đã là 5s (line 248), không cần fix |
| 2A.3 | Fix `ram_gb = 0` trong benchmark JSON | Low | ✅ | `benchmark/run_benchmark_v4.py` — đổi `free -g` → `free -m /1024` |

### 2B — Medium (cần test kỹ sau khi fix)

| # | Bug | Priority | Trạng thái | File |
|---|---|---|---|---|
| 2B.1 | `customers:total` sai khi UPDATE/DELETE | High | ❌ | L117: incr chạy cả khi UPDATE (phải skip); L96-98: thiếu `decr` khi DELETE. `orders:total` cũng cùng vấn đề — `jobs/scala/cdc_redis_consumer.scala` |
| 2B.2 | Grafana datasource UID mismatch sau `stop -v` | Medium | ❌ | `start.sh` (auto-patch chưa implement) |
| 2B.3 | `executor_cores` và `executor_memory_mb` = 0 | Low | ❌ | `monitoring/exporter/metrics_exporter.py` |

### 2C — Hard (cần nghiên cứu thêm)

| # | Bug | Priority | Trạng thái | Ghi chú |
|---|---|---|---|---|
| 2C.1 | Spark batch duration luôn = 0ms | Medium | ❌ | Cần Spark REST API hoặc `StreamingQueryListener` |

---

## Phase 3 — Enhancement

| # | Feature | Priority | Trạng thái | Ghi chú |
|---|---|---|---|---|
| 3.1 | Thêm panel real-time TPS/rate vào Grafana dashboard | High | ❌ | Metrics đã có: `cdc_mysql_insert_rate`, `cdc_mongo_write_rate`, `cdc_lag_total` |
| 3.2 | Benchmark mode `realistic`: 60% INSERT / 30% UPDATE / 10% DELETE | Medium | ❌ | `benchmark/run_benchmark_v4.py` |
| 3.3 | So sánh đúng Scala vs Python (sau khi fix trigger interval) | Medium | ❌ | Phụ thuộc vào 2A.2 |
| 3.4 | Test Kafka partition > 1 | Low | ❌ | Benchmark `partition` mode đã có, chưa test thực tế |
| 3.5 | Spark batch duration thật (via `StreamingQueryListener`) | Low | ❌ | Phức tạp, để sau |
| 3.6 | Multi-table CDC (ngoài customers/orders) | Low | ❌ | Code đã handle, chỉ cần config thêm |
| 3.7 | Grafana alert khi lag > ngưỡng | Low | ❌ | |
| 3.8 | Benchmark trên cloud VM | Low | ❌ | Docs có hướng dẫn tại `docs/VM_SETUP.md` |

---

## Đã hoàn thành (tham khảo)

| Item | Ngày | Ghi chú |
|---|---|---|
| CDC E2E MySQL → Kafka → Spark → MongoDB | 2026-04-19 | Core pipeline |
| CDC E2E → Redis | 2026-04-19 | Hash + counter + sorted set |
| Idempotent upsert (replaceOne + upsert) | 2026-04-19 | |
| Spark checkpoint | 2026-04-19 | |
| Auto-fix Kafka Cluster ID conflict | 2026-05-06 | `start.sh` |
| Rate metrics trong exporter | 2026-05-06 | insert_rate, kafka_rate, mongo_rate, lag |
| Hardware profiles (.env.laptop / .env.server / .env.vm) | 2026-05-06 | |
| Pin versions trong requirements.txt | 2026-05-06 | |
| Tạo .env.example | 2026-05-06 | |
| Windows Git Bash compatibility | 2026-05-06 | python3 fallback, awk fix |
| Demo script + screenshots | 2026-05-06 | |
| Benchmark quick mode (1.11) | 2026-05-07 | 100→90.7, 200→181.2, 500→403.6 rec/s E2E. RAM/Spark=0 là bugs riêng (2A.3, 2B.3) |
| detect_hardware.sh | 2026-05-07 | Windows+Linux, privilege detection, sudo prompt (Linux), smart config calculator từ RAM/CPU |
| start.sh auto-detect profile | 2026-05-07 | Tự chọn laptop/server/vm nếu không truyền --profile |
| start.sh override flags | 2026-05-07 | --partitions=N, --kafka-heap=Xg, --spark-workers=N, --spark-memory=Xg, --spark-cores=N |
| Phase 2A bug fixes | 2026-05-07 | 2A.1: TPS→records/s, 2A.2: trigger đã là 5s, 2A.3: free -m fix |
| docs/CLARIFICATIONS.md | 2026-05-07 | Giải thích TPS vs records/s vs events/s, E2E đo gì, Redis counter bug, spark=0, ram_gb=0 |

---

## Lịch sử

| Ngày | Thay đổi |
|---|---|
| 2026-05-06 | Tạo file TASKS.md, tổng hợp từ KNOWN_ISSUES.md |
| 2026-05-07 | Hoàn thành Phase 1 (1.11 benchmark). Thêm detect_hardware.sh + start.sh enhancements |
| 2026-05-07 | Hoàn thành Phase 2A (2A.1: TPS→records/s, 2A.2: already fixed, 2A.3: ram_gb free -m fix) |
| 2026-05-07 | Thêm docs/CLARIFICATIONS.md. Cập nhật CLAUDE.md: sửa gotchas sai, thêm quy tắc tự update TASKS.md |
