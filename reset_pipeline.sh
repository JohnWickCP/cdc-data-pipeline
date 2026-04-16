#!/bin/bash
# ============================================================
# reset_pipeline.sh — CDC Pipeline Reset Script (Robust Version)
#
# Xử lý mọi tình huống:
#   1. docker compose down (không -v) → volumes còn
#   2. docker compose down -v        → volumes bị xóa → Cluster ID conflict
#   3. Tắt máy thẳng                 → containers crashed
#
# Cách dùng: bash reset_pipeline.sh
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$SCRIPT_DIR/demo"
COMPOSE_DIR="$SCRIPT_DIR/pipeline"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║     CDC PIPELINE — RESET SCRIPT v2      ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ============================================================
# HELPER: chờ container healthy
# ============================================================
wait_healthy() {
  local NAME=$1
  local MAX=$2
  for i in $(seq 1 $MAX); do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$NAME" 2>/dev/null)
    RUNNING=$(docker inspect --format='{{.State.Status}}' "$NAME" 2>/dev/null)
    if [ "$STATUS" = "healthy" ] || ([ "$RUNNING" = "running" ] && [ "$STATUS" = "" ]); then
      return 0
    fi
    sleep 3
  done
  return 1
}

# ============================================================
# BƯỚC 0: Kiểm tra & fix Docker Compose stack
# ============================================================
echo "▶ [0/9] Kiểm tra Docker stack..."

# Dừng Spark + exporter trước khi làm gì
docker exec cdc-spark-master bash -c "pkill -f CdcRedisConsumer 2>/dev/null; exit 0" 2>/dev/null
pkill -f metrics_exporter.py 2>/dev/null || true

# Detect Kafka có bị Cluster ID conflict không
KAFKA_STATUS=$(docker inspect --format='{{.State.Status}}' cdc-kafka 2>/dev/null)
KAFKA_EXIT=$(docker inspect --format='{{.State.ExitCode}}' cdc-kafka 2>/dev/null)

if [ "$KAFKA_STATUS" = "exited" ] && [ "$KAFKA_EXIT" = "1" ]; then
  CLUSTER_ERR=$(docker logs cdc-kafka --tail 20 2>/dev/null | grep -c "InconsistentClusterIdException" || echo "0")
  if [ "$CLUSTER_ERR" -gt "0" ]; then
    echo "  ⚠️  Phát hiện Kafka Cluster ID conflict — xóa volumes và recreate..."
    cd "$COMPOSE_DIR"
    docker compose down -v > /dev/null 2>&1
    docker compose up -d > /dev/null 2>&1
    echo "  → Chờ tất cả service khởi động (60s)..."
    sleep 60
    cd "$SCRIPT_DIR"
    echo "  ✅ Stack đã recreate xong"
  fi
else
  # Kiểm tra các container cần thiết có đang chạy không
  NEED_START=0
  for SVC in cdc-kafka cdc-debezium cdc-spark-master cdc-mysql cdc-mongodb cdc-redis; do
    ST=$(docker inspect --format='{{.State.Status}}' "$SVC" 2>/dev/null)
    if [ "$ST" != "running" ]; then
      NEED_START=1
      echo "  ⚠️  $SVC chưa chạy (status: ${ST:-not found})"
    fi
  done

  if [ "$NEED_START" = "1" ]; then
    echo "  → Khởi động Docker Compose..."
    cd "$COMPOSE_DIR"
    docker compose up -d > /dev/null 2>&1
    echo "  → Chờ tất cả service khởi động (50s)..."
    sleep 50
    cd "$SCRIPT_DIR"
  else
    echo "  ✅ Tất cả containers đang chạy"
  fi
fi

# ============================================================
# BƯỚC 1: Dừng Spark job
# ============================================================
echo ""
echo "▶ [1/9] Dừng Spark job cũ..."
docker exec cdc-spark-master bash -c "pkill -f CdcRedisConsumer 2>/dev/null; exit 0"
sleep 3
echo "  ✅ Spark job đã dừng"

# ============================================================
# BƯỚC 2: Dừng metrics exporter
# ============================================================
echo ""
echo "▶ [2/9] Dừng metrics exporter cũ..."
pkill -f metrics_exporter.py 2>/dev/null || true
sleep 1
echo "  ✅ Metrics exporter đã dừng"

# ============================================================
# BƯỚC 3: Xóa connector + Spark checkpoint + Kafka CDC topics
# ============================================================
echo ""
echo "▶ [3/9] Xóa connector + checkpoint + Kafka topics CDC..."

curl -s -X DELETE http://localhost:8083/connectors/mysql-inventory-connector > /dev/null 2>&1
sleep 3

# Xóa Spark checkpoint
docker exec cdc-spark-master bash -c "rm -rf /tmp/spark-checkpoint" 2>/dev/null

# Xóa chỉ CDC topics — KHÔNG xóa connect_offsets/configs/statuses
for TOPIC in inventory.inventory.customers inventory.inventory.orders inventory schema-changes.inventory; do
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic "$TOPIC" 2>/dev/null || true
done
sleep 5
echo "  ✅ Connector + checkpoint + Kafka CDC topics đã xóa"

# ============================================================
# BƯỚC 4: Flush Redis
# ============================================================
echo ""
echo "▶ [4/9] Flush Redis..."
docker exec cdc-redis redis-cli FLUSHALL
echo "  ✅ Redis đã flush"

# ============================================================
# BƯỚC 5: Xóa data MongoDB
# ============================================================
echo ""
echo "▶ [5/9] Xóa data MongoDB..."
docker exec cdc-mongodb mongosh --quiet --eval "
  db.getSiblingDB('inventory').customers.deleteMany({});
  db.getSiblingDB('inventory').orders.deleteMany({});
  print('MongoDB cleared');
"
echo "  ✅ MongoDB đã xóa"

# ============================================================
# BƯỚC 6: Reset MySQL
# ============================================================
echo ""
echo "▶ [6/9] Reset MySQL..."
docker exec -i cdc-mysql mysql -uroot -proot < "$DEMO_DIR/demodata.sql"
echo "  ✅ MySQL đã reset (3 customers, 4 orders)"

# ============================================================
# BƯỚC 7: Restart Debezium + register connector
# ============================================================
echo ""
echo "▶ [7/9] Restart Debezium + register connector..."

docker restart cdc-debezium > /dev/null
echo "  → Chờ Debezium khởi động (30s)..."
sleep 30

# Luôn dùng "initial" vì ta đã xóa schema-changes.inventory ở bước 3.
# schema_only_recovery chỉ hoạt động khi topic đó còn tồn tại — không bao giờ đúng sau reset.
SNAPSHOT_MODE="initial"
echo "  → Snapshot mode: $SNAPSHOT_MODE"

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"mysql-inventory-connector\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
      \"tasks.max\": \"1\",
      \"database.hostname\": \"cdc-mysql\",
      \"database.port\": \"3306\",
      \"database.user\": \"root\",
      \"database.password\": \"root\",
      \"database.server.id\": \"184054\",
      \"topic.prefix\": \"inventory\",
      \"database.include.list\": \"inventory\",
      \"table.include.list\": \"inventory.customers,inventory.orders\",
      \"schema.history.internal.kafka.bootstrap.servers\": \"cdc-kafka:29092\",
      \"schema.history.internal.kafka.topic\": \"schema-changes.inventory\",
      \"snapshot.mode\": \"$SNAPSHOT_MODE\",
      \"include.schema.changes\": \"true\",
      \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"key.converter.schemas.enable\": \"false\",
      \"value.converter.schemas.enable\": \"false\"
    }
  }")

echo "  → HTTP response: $RESPONSE"

# Chờ connector RUNNING — tối đa 40s, nếu FAILED thì tự retry 1 lần
echo "  → Chờ connector RUNNING..."
CONNECTOR_OK=0
RETRIED=0
for i in $(seq 1 20); do
  CONN_JSON=$(curl -s http://localhost:8083/connectors/mysql-inventory-connector/status 2>/dev/null)
  STATE=$(echo "$CONN_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tasks'][0]['state'])" 2>/dev/null)

  if [ "$STATE" = "RUNNING" ]; then
    echo "  ✅ Connector RUNNING"
    CONNECTOR_OK=1
    break
  elif [ "$STATE" = "FAILED" ] && [ "$RETRIED" = "0" ]; then
    TRACE=$(echo "$CONN_JSON" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(d['tasks'][0].get('trace','')[:300])
" 2>/dev/null)
    echo "  ⚠️  Connector FAILED — tự retry..."
    echo "  Lỗi: $TRACE"
    RETRIED=1
    docker restart cdc-debezium > /dev/null
    sleep 25
    curl -s -X DELETE http://localhost:8083/connectors/mysql-inventory-connector > /dev/null 2>&1
    sleep 2
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d "{
        \"name\": \"mysql-inventory-connector\",
        \"config\": {
          \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
          \"tasks.max\": \"1\",
          \"database.hostname\": \"cdc-mysql\",
          \"database.port\": \"3306\",
          \"database.user\": \"root\",
          \"database.password\": \"root\",
          \"database.server.id\": \"184054\",
          \"topic.prefix\": \"inventory\",
          \"database.include.list\": \"inventory\",
          \"table.include.list\": \"inventory.customers,inventory.orders\",
          \"schema.history.internal.kafka.bootstrap.servers\": \"cdc-kafka:29092\",
          \"schema.history.internal.kafka.topic\": \"schema-changes.inventory\",
          \"snapshot.mode\": \"initial\",
          \"include.schema.changes\": \"true\",
          \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
          \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
          \"key.converter.schemas.enable\": \"false\",
          \"value.converter.schemas.enable\": \"false\"
        }
      }" > /dev/null 2>&1
    sleep 5
    continue
  fi

  echo "  → [$i/20] State: ${STATE:-pending}..."
  sleep 2
done

if [ "$CONNECTOR_OK" = "0" ]; then
  echo ""
  echo "  ❌ Connector vẫn chưa RUNNING — kiểm tra thủ công:"
  echo "     docker logs cdc-debezium --tail 30"
  echo "     curl -s http://localhost:8083/connectors/mysql-inventory-connector/status | python3 -m json.tool"
  echo ""
fi

echo "  → Chờ Debezium tạo Kafka topics (15s)..."
sleep 15

# ============================================================
# BƯỚC 8: Xác nhận Kafka topics
# ============================================================
echo ""
echo "▶ [8/9] Kiểm tra Kafka topics..."

TOPICS_OK=0
for attempt in 1 2; do
  TOPICS=$(docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
  if echo "$TOPICS" | grep -q "inventory.inventory.customers"; then
    echo "  ✅ Kafka topics CDC đã tạo thành công"
    TOPICS_OK=1
    break
  else
    echo "  ⚠️  Topics chưa tạo — chờ thêm 15s..."
    sleep 15
  fi
done

if [ "$TOPICS_OK" = "0" ]; then
  echo "  ❌ Topics vẫn chưa tạo. Kiểm tra connector:"
  echo "     curl -s http://localhost:8083/connectors/mysql-inventory-connector/status | python3 -m json.tool"
fi

# ============================================================
# BƯỚC 9: Khởi động Spark job + metrics exporter
# ============================================================
echo ""
echo "▶ [9/9] Khởi động Spark job + Metrics Exporter..."

docker exec -d cdc-spark-master /bin/bash -c \
  "/opt/spark/bin/spark-submit \
    --class CdcRedisConsumer \
    --master spark://cdc-spark-master:7077 \
    /opt/spark/jobs/scala/target/scala-2.12/cdc-mysql-to-mongodb-redis_2.12-1.0.jar \
    > /tmp/spark_metrics.log 2>&1"

nohup python3 "$SCRIPT_DIR/metrics_exporter.py" \
  > "$SCRIPT_DIR/metrics_exporter.log" 2>&1 &
EXPORTER_PID=$!

echo "  → Chờ pipeline khởi động (20s)..."
sleep 20

if kill -0 $EXPORTER_PID 2>/dev/null; then
  echo "  ✅ Spark job đã khởi động"
  echo "  ✅ Metrics exporter đang chạy (PID=$EXPORTER_PID)"
else
  echo "  ✅ Spark job đã khởi động"
  echo "  ⚠️  Metrics exporter lỗi — kiểm tra: cat $SCRIPT_DIR/metrics_exporter.log"
fi

# ============================================================
# KIỂM TRA CUỐI — chờ MongoDB sync tối đa 45s
# ============================================================
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║            KIỂM TRA SAU RESET            ║"
echo "╚══════════════════════════════════════════╝"

echo "  → Chờ MongoDB sync (tối đa 45s)..."
MONGO_COUNT="0"
for i in $(seq 1 9); do
  MONGO_COUNT=$(docker exec cdc-mongodb mongosh --quiet --eval \
    "db.getSiblingDB('inventory').customers.countDocuments({})" 2>/dev/null | tail -1)
  if [ "$MONGO_COUNT" = "3" ]; then
    break
  fi
  sleep 5
done

MYSQL_COUNT=$(docker exec cdc-mysql mysql -uroot -proot -sN -e \
  "SELECT COUNT(*) FROM inventory.customers;" 2>/dev/null)
REDIS_KEYS=$(docker exec cdc-redis redis-cli DBSIZE)
EXPORTER_UP=$(curl -s http://localhost:8000/metrics 2>/dev/null \
  | grep -c "cdc_pipeline_up" || echo "0")

echo ""
echo "  MySQL      customers : $MYSQL_COUNT"
echo "  MongoDB    customers : $MONGO_COUNT"
echo "  Redis      keys      : $REDIS_KEYS"
echo "  Exporter   status    : $([ "$EXPORTER_UP" -gt "0" ] && echo '✅ OK' || echo '⚠️  chưa sẵn sàng')"
echo ""

if [ "$MYSQL_COUNT" = "3" ] && [ "$MONGO_COUNT" = "3" ]; then
  echo "  ✅ Pipeline hoạt động bình thường — sẵn sàng demo!"
elif [ "$MYSQL_COUNT" = "3" ] && [ "$MONGO_COUNT" = "0" ]; then
  echo "  ⚠️  MongoDB chưa sync. Chờ thêm 20s rồi kiểm tra:"
  echo "      docker exec cdc-mongodb mongosh --quiet --eval \"db.getSiblingDB('inventory').customers.countDocuments()\""
  echo "  Nếu vẫn 0, xem Spark log:"
  echo "      docker exec cdc-spark-master tail -30 /tmp/spark_metrics.log"
else
  echo "  ⚠️  MySQL=$MYSQL_COUNT MongoDB=$MONGO_COUNT — kiểm tra lại pipeline"
fi

echo ""
echo "🎉 Reset hoàn tất!"
echo ""
echo "   📊 Grafana : http://localhost:3000  (admin/admin)"
echo "   🔥 Spark UI: http://localhost:8080"
echo "   📈 Metrics : http://localhost:8000/metrics"
echo ""