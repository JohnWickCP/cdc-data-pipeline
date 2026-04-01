#!/bin/bash
# ============================================================
# reset_pipeline.sh
# Dọn sạch toàn bộ state cũ của CDC pipeline.
# Dùng trước mỗi lần chạy test mới để đảm bảo môi trường sạch.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$SCRIPT_DIR/demo"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║        CDC PIPELINE — RESET SCRIPT       ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ============================================================
# BƯỚC 1: Dừng Spark job
# ============================================================
echo "▶ [1/9] Dừng Spark job cũ..."
docker exec cdc-spark-master bash -c "pkill -f CdcRedisConsumer 2>/dev/null; exit 0"
sleep 5
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
# BƯỚC 3: Xóa Debezium connector + checkpoint + Kafka topics CDC
# ============================================================
echo ""
echo "▶ [3/9] Xóa connector + checkpoint + Kafka topics CDC..."

# Xóa connector trước
curl -s -X DELETE http://localhost:8083/connectors/mysql-inventory-connector > /dev/null 2>&1
sleep 3

# Xóa checkpoint Spark
docker exec cdc-spark-master bash -c "rm -rf /tmp/spark-checkpoint" 2>/dev/null

# Xóa chỉ topics CDC data — KHÔNG xóa connect_offsets/configs/statuses
for TOPIC in inventory.inventory.customers inventory.inventory.orders inventory schema-changes.inventory; do
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic "$TOPIC" 2>/dev/null || true
done
sleep 3
echo "  ✅ Connector + checkpoint + Kafka topics CDC đã xóa"

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
# BƯỚC 6: Reset MySQL về dữ liệu demo gốc
# ============================================================
echo ""
echo "▶ [6/9] Reset MySQL..."
docker exec -i cdc-mysql mysql -uroot -proot < "$DEMO_DIR/demodata.sql"
echo "  ✅ MySQL đã reset (3 customers, 4 orders)"

# ============================================================
# BƯỚC 7: Restart Debezium + register connector (recovery mode)
# ============================================================
echo ""
echo "▶ [7/9] Restart Debezium + register connector..."

docker restart cdc-debezium > /dev/null
echo "  → Chờ Debezium khởi động (30s)..."
sleep 30

# Register với schema_only_recovery để Debezium không cần history cũ
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-inventory-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": "cdc-mysql",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "root",
      "database.server.id": "184054",
      "topic.prefix": "inventory",
      "database.include.list": "inventory",
      "table.include.list": "inventory.customers,inventory.orders",
      "schema.history.internal.kafka.bootstrap.servers": "cdc-kafka:29092",
      "schema.history.internal.kafka.topic": "schema-changes.inventory",
      "schema.history.internal.store.only.captured.tables.ddl": "true",
      "snapshot.mode": "schema_only_recovery",
      "include.schema.changes": "true",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }')

echo "  → Chờ connector RUNNING..."
for i in $(seq 1 15); do
  STATE=$(curl -s http://localhost:8083/connectors/mysql-inventory-connector/status \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tasks'][0]['state'])" 2>/dev/null)
  if [ "$STATE" = "RUNNING" ]; then
    echo "  ✅ Connector RUNNING"
    break
  fi
  sleep 2
done

echo "  → Chờ Debezium tạo Kafka topics (15s)..."
sleep 15

# ============================================================
# BƯỚC 8: Xác nhận topics đã tạo
# ============================================================
echo ""
echo "▶ [8/9] Kiểm tra Kafka topics..."
TOPICS=$(docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list)
if echo "$TOPICS" | grep -q "inventory.inventory.customers"; then
  echo "  ✅ Kafka topics CDC đã tạo thành công"
else
  echo "  ⚠️  Topics chưa tạo — chờ thêm 10s..."
  sleep 10
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
  echo "  ⚠️  Metrics exporter lỗi — kiểm tra: cat metrics_exporter.log"
fi

# ============================================================
# KIỂM TRA NHANH
# ============================================================
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║            KIỂM TRA SAU RESET            ║"
echo "╚══════════════════════════════════════════╝"

MYSQL_COUNT=$(docker exec cdc-mysql mysql -uroot -proot -sN -e \
  "SELECT COUNT(*) FROM inventory.customers;" 2>/dev/null)
MONGO_COUNT=$(docker exec cdc-mongodb mongosh --quiet --eval \
  "db.getSiblingDB('inventory').customers.countDocuments({})" 2>/dev/null | tail -1)
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
  echo "  ✅ MySQL và MongoDB khớp nhau (3 customers) — sẵn sàng test!"
elif [ "$MYSQL_COUNT" = "3" ] && [ "$MONGO_COUNT" = "0" ]; then
  echo "  ⚠️  MongoDB chưa sync — chờ thêm 15s rồi kiểm tra:"
  echo "      docker exec cdc-mongodb mongosh --quiet --eval \"db.getSiblingDB('inventory').customers.countDocuments()\""
else
  echo "  ⚠️  MySQL=$MYSQL_COUNT MongoDB=$MONGO_COUNT"
fi

echo ""
echo "🎉 Reset hoàn tất!"
echo ""
echo "   📊 Grafana : http://localhost:3000"
echo "   🔥 Spark UI: http://localhost:8080"
echo "   📈 Metrics : http://localhost:8000/metrics"
echo ""