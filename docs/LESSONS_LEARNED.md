# LESSONS LEARNED — CDC Data Pipeline

Ghi lại các vấn đề thực tế gặp phải, tư duy giải quyết, và bài học rút ra.
Mục đích: tài liệu này là **minh chứng kỹ thuật** — không phải lý thuyết sách giáo khoa,
mà là những gì thực sự xảy ra khi build hệ thống này.

---

## 1. Debezium op codes và lỗi Redis counter

### Vấn đề

`customers:total` trong Redis cứ tăng mãi dù có UPDATE hoặc DELETE. Sau khi chạy
benchmark (vài trăm INSERT), `customers:total = 25717` trong khi MySQL chỉ có 4 bản ghi.

### Nguyên nhân gốc rễ

Debezium phát ra 4 loại event:

| op code | Ý nghĩa |
|---|---|
| `c` | CREATE — INSERT mới |
| `r` | READ — snapshot ban đầu khi connector khởi động |
| `u` | UPDATE |
| `d` | DELETE |

Code Scala ban đầu viết:

```scala
if (op == "d") {
  customersCol.deleteOne(...)
  pipe.del(s"customer:$id")
  // thiếu: pipe.decr("customers:total")
} else {
  // upsert MongoDB...
  pipe.hset(s"customer:$id", hashData)
  pipe.incr("customers:total")   // ← BUG: tăng cả khi op == "u"
}
```

Mỗi UPDATE tăng counter một lần → counter bị drift hoàn toàn so với thực tế.

### Cách fix

```scala
if (op == "d") {
  customersCol.deleteOne(...)
  pipe.del(s"customer:$id")
  pipe.decr("customers:total")              // ← thêm
} else {
  // upsert MongoDB...
  pipe.hset(s"customer:$id", hashData)
  if (op == "c" || op == "r") pipe.incr("customers:total")  // ← gate điều kiện
}
```

### Bài học

- **Luôn enumerate rõ ràng từng op code** thay vì dùng `else` chung chung.
- Redis counter chỉ có ý nghĩa nếu được tăng/giảm **đối xứng**: mỗi `c`/`r` tăng 1,
  mỗi `d` giảm 1, `u` không đụng.
- Sau khi fix, phải **reset counter thủ công** về giá trị đúng trước khi test:
  `docker exec cdc-redis redis-cli SET customers:total <N>`
- Cách verify: INSERT → +1, UPDATE → giữ nguyên, DELETE → -1. Test đủ 3 cases.

---

## 2. Rebuild Scala JAR trên Windows không có sbt

### Vấn đề

Cần rebuild JAR sau khi sửa Scala code, nhưng:
- Không có `sbt` hay `scalac` trên máy Windows host
- Spark container không có sbt
- Image `sbtscala/scala-sbt:eclipse-temurin-17.0.9_9_1.9.7_2.12.18` không tồn tại trên Docker Hub

### Tư duy giải quyết

Cần một môi trường có JDK + sbt, chạy được trên Windows, có thể mount source code vào.
Docker container tự build sbt at runtime là giải pháp gọn nhất:

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "d:/DATN/cdc-data-pipeline/jobs/scala:/work" \
  -v "d:/DATN/.sbt-cache/ivy2:/root/.ivy2" \      # cache tái dùng
  -v "d:/DATN/.sbt-cache/sbt:/root/.sbt" \         # cache tái dùng
  -w /work \
  eclipse-temurin:17-jdk \
  bash -c "curl -fsSL https://github.com/sbt/sbt/releases/download/v1.9.7/sbt-1.9.7.tgz \
    | tar -xz -C /tmp && /tmp/sbt/bin/sbt package"
```

Lần đầu mất ~5 phút (download sbt + dependencies), lần sau ~30 giây (cache).

### Cấu hình build.sbt cần thiết

Source file đặt thẳng ở root project (không dùng `src/main/scala/`), nên cần thêm:

```scala
Compile / unmanagedSourceDirectories += baseDirectory.value
```

Nếu thiếu dòng này, sbt không tìm thấy `.scala` file và build ra JAR trống.

### JAR size là bao nhiêu là đúng?

JAR chỉ ~15–17K là **đúng**. Tất cả dependencies (Spark, Kafka, MongoDB, Redis) đều
khai báo `% "provided"` trong build.sbt — Spark cung cấp chúng tại runtime qua
`--packages`. JAR chỉ chứa compiled class files của code mình viết.

---

## 3. Docker image baked vs volume mount

### Vấn đề

Sửa `metrics_exporter.py` trên host, `docker compose restart metrics-exporter`,
nhưng container vẫn chạy code cũ.

### Nguyên nhân

Metrics exporter dùng Dockerfile (baked image):

```dockerfile
COPY metrics_exporter.py /app/metrics_exporter.py
```

`docker compose restart` chỉ khởi động lại container từ image hiện có — không rebuild.
File mới trên host không được copy vào image cũ.

### Cách verify

```bash
docker exec cdc-metrics-exporter grep -n "code_bạn_vừa_thêm" /app/metrics_exporter.py
```
Nếu không ra kết quả → container đang dùng code cũ.

### Cách fix

```bash
cd pipeline/
docker compose build metrics-exporter
docker compose up -d metrics-exporter
```

### Quy tắc tổng quát

| Service type | Khi sửa code cần... |
|---|---|
| Volume-mounted (ví dụ: `./src:/app/src`) | `docker compose restart <service>` |
| Baked image (`COPY` trong Dockerfile) | `docker compose build <service> && docker compose up -d <service>` |

Khi không chắc: check `docker-compose.yml` xem service có `volumes:` mount code không.

---

## 4. Spark JAR mount path sai

### Vấn đề

Submit Spark job với đường dẫn `/opt/spark/work-dir/cdc-mysql-to-mongodb-redis_2.12-1.0.jar`
→ job bị fail silently (submit với `-d` nên không thấy lỗi).

### Nguyên nhân

Nhầm path. `docker-compose.yml` mount JAR tại:

```yaml
volumes:
  - ../jobs:/opt/spark/jobs   # ← đây, không phải work-dir
```

### Cách verify path trước khi submit

```bash
MSYS_NO_PATHCONV=1 docker exec cdc-spark-master ls //opt/spark/jobs/
```

### Submit đúng

```bash
MSYS_NO_PATHCONV=1 docker exec -d cdc-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://cdc-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,... \
  /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar
```

---

## 5. MSYS_NO_PATHCONV=1 trên Windows Git Bash

### Vấn đề

Git Bash trên Windows tự động convert Unix paths trong lệnh:

```bash
docker exec cdc-spark-master ls /opt/spark/jobs/
# → Error: cannot access 'C:/Program Files/Git/opt/spark/jobs/'
```

Git Bash thấy `/opt/...` và nghĩ đó là đường dẫn tương đối từ Git installation root.

### Cách fix

**Cách 1 — Prefix `MSYS_NO_PATHCONV=1`:**
```bash
MSYS_NO_PATHCONV=1 docker exec cdc-spark-master ls /opt/spark/jobs/
```

**Cách 2 — Double slash `//`:**
```bash
docker exec cdc-spark-master ls //opt/spark/jobs/
```

**Cách 3 — Trong docker run volumes:**
```bash
MSYS_NO_PATHCONV=1 docker run -v "d:/DATN/project:/work" ...
# Dùng Windows path (d:/...) cho host side, MSYS_NO_PATHCONV cho container side
```

### Những lệnh bị ảnh hưởng

- `docker exec <container> /path/command`
- `docker run -v /host/path:/container/path`
- `curl` với URL có path dạng `/api/v1/...` (hiếm hơn)

---

## 6. Kill Spark job đúng cách

### Vấn đề

Gọi Spark REST API để kill job:
```bash
curl -X POST "http://localhost:8080/app/kill/?id=app-xxx&terminate=true"
```

Spark Master xóa app khỏi registry → `activeapps` trả về rỗng → tưởng job đã chết.
Nhưng process driver JVM vẫn còn sống trong container, vẫn đọc Kafka, vẫn ghi Redis.

### Tại sao nguy hiểm

Hai driver process chạy song song cùng đọc một Kafka topic → race condition → dữ liệu
duplicate trong MongoDB, counter Redis bị cộng đôi.

### Cách kill đúng

**Bước 1:** Kill qua REST API (gỡ khỏi master registry)
```bash
docker exec cdc-spark-master curl -X POST \
  "http://localhost:8080/app/kill/?id=app-xxx&terminate=true"
```

**Bước 2:** Kill process thật
```bash
# Tìm PID
docker exec cdc-spark-master ps aux | grep spark-submit | grep -v grep

# Kill
docker exec cdc-spark-master kill <PID>
```

**Verify:**
```bash
docker exec cdc-spark-master ps aux | grep java | grep -v grep | wc -l
# Phải là: 1 (master) + N (job mới nếu đã submit)
```

---

## 7. StreamingQueryListener — đo Spark batch duration từ bên trong

### Vấn đề

`cdc_spark_batch_duration_ms` luôn = 0. Không có cách nào đo batch duration từ bên
ngoài qua Spark REST API cho Structured Streaming.

### Các cách tiếp cận đã xem xét

| Cách | Khả thi? | Lý do |
|---|---|---|
| Spark REST API `/api/v1/applications/.../streaming/statistics` | Không | API này chỉ có cho DStream (Spark Streaming cũ), không có cho Structured Streaming |
| JMX metrics | Phức tạp | Cần cấu hình JMX exporter, thêm dependency |
| `StreamingQueryListener` trong Scala job | **Chọn** | Built-in, clean, không dependency mới |

### Giải pháp: StreamingQueryListener + Redis làm side channel

**Scala (driver side):**
```scala
spark.streams.addListener(new StreamingQueryListener {
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val triggerMs = Option(event.progress.durationMs.get("triggerExecution"))
      .map(_.longValue()).getOrElse(0L)
    if (triggerMs > 0) {
      val jedis = new Jedis(REDIS_HOST, REDIS_PORT)
      try { jedis.set("spark:batch_duration_ms", triggerMs.toString) }
      finally { jedis.close() }
    }
  }
  // ... onQueryStarted, onQueryTerminated
})
```

**Python exporter (collector side):**
```python
# Trong collect_redis():
batch_dur = r.get("spark:batch_duration_ms")
if batch_dur is not None:
    spark_batch_duration_ms.set(float(batch_dur))
```

### Tại sao tạo Jedis connection mới mỗi lần?

`StreamingQueryListener.onQueryProgress` chạy trên **listener thread** riêng biệt,
khác với thread chạy `processBatch`. Jedis (và phần lớn Redis client) **không thread-safe**,
không thể share connection giữa các thread. Tạo connection mới mỗi batch (mỗi 5s)
là đúng và overhead không đáng kể.

### Ý nghĩa của `triggerExecution`

`event.progress.durationMs` là một Map với nhiều keys:

| Key | Ý nghĩa |
|---|---|
| `triggerExecution` | **Tổng thời gian batch** (wall clock từ trigger đến commit) |
| `getBatch` | Lấy dữ liệu từ source (Kafka) |
| `queryPlanning` | Lập kế hoạch query |
| `addBatch` | Thực thi `processBatch()` — phần tốn time nhất |
| `commitOffsets` | Commit offset về Kafka |

`triggerExecution` là metric có ý nghĩa nhất: nếu > trigger interval (5000ms) thì
pipeline bắt đầu bị lag.

### Batch duration bao nhiêu là tốt?

```
trigger interval = 5000ms

batch_duration_ms < 500ms   → rất khỏe, 90% idle
batch_duration_ms = 1000ms  → khỏe, ~20% utilization
batch_duration_ms = 3000ms  → OK, bắt đầu theo dõi
batch_duration_ms > 5000ms  → LAG — queue Kafka tích tụ
```

Trong pipeline này khi idle: ~350–1300ms. Khi benchmark 500 records/s sẽ tăng.

---

## 8. Spark packages không được cache giữa các lần submit

### Vấn đề

Mỗi lần submit Spark job mới với `--packages`, cần 3–5 phút để download packages
dù đã chạy trước đó.

### Nguyên nhân

Packages được cache trong `~/.ivy2/cache/` của user chạy spark-submit **bên trong container**.
Spark container không có volume mount cho ivy2 cache, nên mỗi lần container restart
hoặc submit mới, cache bị mất.

### Giải pháp nếu muốn cache

Thêm volume mount vào `docker-compose.yml` cho spark-master:

```yaml
spark-master:
  volumes:
    - ../jobs:/opt/spark/jobs
    - spark-ivy2-cache:/home/spark/.ivy2   # ← thêm dòng này
```

Hoặc chấp nhận 3–5 phút chờ lần đầu và không thay đổi gì (acceptable cho dev/test).

### Cách theo dõi tiến độ download

```bash
# Xem CPU của driver process — nếu đang download thì CPU cao
docker exec cdc-spark-master ps -p <PID> -o pid,%cpu,%mem,etime,comm

# Xem file descriptors — nếu đã xong và đang chạy thì sẽ có nhiều .jar open
docker exec cdc-spark-master ls -la /proc/<PID>/fd/ | grep ".jar" | wc -l
```

---

## Tổng kết — Nguyên tắc debug hệ thống này

1. **Verify file thật trong container** trước khi kết luận code đã được deploy:
   `docker exec <container> grep "keyword" /path/to/file`

2. **Check volume mounts trong docker-compose.yml** trước khi dùng path trong docker exec.

3. **Prefx MSYS_NO_PATHCONV=1** cho mọi docker command có Unix path trong Git Bash.

4. **Sau khi kill Spark job**, luôn check process còn sống không:
   `docker exec cdc-spark-master ps aux | grep java | grep -v grep`

5. **Redis counter drift** — nếu thấy số Redis vô lý lớn, reset về giá trị đúng từ MySQL:
   `docker exec cdc-redis redis-cli SET customers:total $(docker exec cdc-mysql mysql -uroot -proot inventory -se "SELECT COUNT(*) FROM customers;" 2>/dev/null)`

6. **Test counter đủ 3 cases:** INSERT (+1), UPDATE (0), DELETE (-1) — không chỉ test INSERT.

7. **Batch duration > trigger interval** = dấu hiệu đầu tiên của lag, check ngay Kafka consumer lag.
