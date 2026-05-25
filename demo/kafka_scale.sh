#!/usr/bin/env bash
# kafka_scale.sh — Demo: scale Kafka brokers theo chiều ngang
#
# Usage:
#   bash demo/kafka_scale.sh status     # xem broker hiện tại + partition info
#   bash demo/kafka_scale.sh add        # thêm kafka-2 + kafka-3
#   bash demo/kafka_scale.sh rebalance  # tăng partitions → restart Spark job
#   bash demo/kafka_scale.sh remove     # dừng kafka-2 + kafka-3
#   bash demo/kafka_scale.sh bench      # chạy benchmark ở mỗi cấu hình broker

set -euo pipefail

# ── Config ─────────────────────────────────────────────────────────────
BOOTSTRAP_1="localhost:29092"
TOPICS=(
    "inventory.inventory.customers"
    "inventory.inventory.orders"
    "connect_configs"
    "connect_offsets"
    "connect_statuses"
)
# Bootstrap của toàn cluster khi có 3 brokers
BOOTSTRAP_ALL="cdc-kafka:29092,cdc-kafka-2:29093,cdc-kafka-3:29094"
DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_NAME="cdc-mysql-connector"

# ── Helpers ─────────────────────────────────────────────────────────────
log()  { echo "[kafka_scale] $*"; }
warn() { echo "[kafka_scale] WARN: $*" >&2; }
fail() { echo "[kafka_scale] ERROR: $*" >&2; exit 1; }

broker_count() {
    docker ps --filter "name=cdc-kafka" --format "{{.Names}}" \
        | grep -c "cdc-kafka" 2>/dev/null || echo 0
}

wait_healthy() {
    local container="$1"
    local max=60
    log "Waiting for $container to be healthy..."
    for i in $(seq 1 $max); do
        status=$(docker inspect --format "{{.State.Health.Status}}" "$container" 2>/dev/null || echo "none")
        [ "$status" = "healthy" ] && return 0
        echo -n "."
        sleep 2
    done
    echo ""
    warn "$container not healthy after ${max}× 2s — continuing anyway"
}

topic_partitions() {
    local topic="$1"
    docker exec cdc-kafka kafka-topics \
        --bootstrap-server "$BOOTSTRAP_1" \
        --describe --topic "$topic" 2>/dev/null \
        | grep "PartitionCount" | awk '{print $4}' | head -1
}

# ── status ──────────────────────────────────────────────────────────────
cmd_status() {
    echo ""
    echo "═══════════════════════════════════════════"
    echo " Kafka Cluster Status"
    echo "═══════════════════════════════════════════"

    echo ""
    echo "── Broker containers ──────────────────────"
    docker ps --filter "name=cdc-kafka" --format \
        "  {{.Names}}\t{{.Status}}" | column -t

    echo ""
    echo "── Topic partitions ───────────────────────"
    for topic in "${TOPICS[@]}"; do
        parts=$(topic_partitions "$topic" 2>/dev/null || echo "?")
        printf "  %-45s  %s partition(s)\n" "$topic" "$parts"
    done

    echo ""
    echo "── Debezium bootstrap servers ─────────────"
    curl -sf "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/config" 2>/dev/null \
        | python3 -c "import sys,json; c=json.load(sys.stdin); print(' ', c.get('database.history.kafka.bootstrap.servers', c.get('bootstrap.servers','?')))" \
        || echo "  (cannot reach Debezium)"

    echo ""
    echo "Active brokers: $(broker_count)"
    echo "═══════════════════════════════════════════"
}

# ── add ─────────────────────────────────────────────────────────────────
cmd_add() {
    local current
    current=$(broker_count)

    if [ "$current" -ge 3 ]; then
        log "Already running $current brokers — nothing to do."
        cmd_status; return
    fi

    log "Starting kafka-2 and kafka-3 (profile: multi-broker)..."
    COMPOSE_PROFILES=multi-broker docker compose up -d kafka-2 kafka-3

    wait_healthy "cdc-kafka-2"
    wait_healthy "cdc-kafka-3"

    log "Brokers 2 and 3 are up."
    log "Run 'bash demo/kafka_scale.sh rebalance' to increase partitions and restart Spark."
    cmd_status
}

# ── rebalance ────────────────────────────────────────────────────────────
cmd_rebalance() {
    local current
    current=$(broker_count)

    if [ "$current" -lt 2 ]; then
        fail "Only $current broker running. Run 'add' first."
    fi

    local new_partitions=3
    log "Increasing partitions to $new_partitions on CDC topics..."

    for topic in "inventory.inventory.customers" "inventory.inventory.orders"; do
        current_parts=$(topic_partitions "$topic" 2>/dev/null || echo 0)
        if [ "${current_parts:-0}" -ge "$new_partitions" ]; then
            log "  $topic already has $current_parts partition(s) — skip"
            continue
        fi

        log "  $topic: $current_parts → $new_partitions partitions"
        docker exec cdc-kafka kafka-topics \
            --bootstrap-server "$BOOTSTRAP_1" \
            --alter --topic "$topic" \
            --partitions "$new_partitions" \
            2>/dev/null || warn "  Could not alter $topic (may not exist yet)"
    done

    log "Updating Debezium bootstrap.servers → all 3 brokers..."
    curl -sf -X PUT "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/config" \
        -H "Content-Type: application/json" \
        -d "{
            \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
            \"database.hostname\": \"cdc-mysql\",
            \"database.port\": \"3306\",
            \"database.user\": \"root\",
            \"database.password\": \"root\",
            \"database.server.id\": \"1\",
            \"database.include.list\": \"inventory\",
            \"topic.prefix\": \"inventory\",
            \"schema.history.internal.kafka.bootstrap.servers\": \"$BOOTSTRAP_ALL\",
            \"schema.history.internal.kafka.topic\": \"schema-changes.inventory\",
            \"include.schema.changes\": \"true\",
            \"decimal.handling.mode\": \"string\",
            \"tombstones.on.delete\": \"false\"
        }" > /dev/null 2>&1 \
        && log "  Debezium config updated." \
        || warn "  Could not update Debezium config — update manually if needed"

    log "Restarting Spark job to pick up new partitions..."
    # Kill driver process
    docker exec cdc-spark-master pkill -f CdcRedisConsumer 2>/dev/null || true
    sleep 3
    # Resubmit
    docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
        --class CdcRedisConsumer \
        --master spark://cdc-spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0 \
        /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar \
        2>/dev/null
    log "  Spark job resubmitted (checkpoint preserved)."

    echo ""
    log "Rebalance complete. Wait ~30s for Spark to resume processing."
    cmd_status
}

# ── remove ───────────────────────────────────────────────────────────────
cmd_remove() {
    local current
    current=$(broker_count)

    if [ "$current" -le 1 ]; then
        log "Already at 1 broker — nothing to remove."
        return
    fi

    warn "Removing brokers 2 and 3."
    warn "Existing topics still have $( topic_partitions inventory.inventory.customers ) partitions."
    warn "Partitions on brokers 2/3 will become UNAVAILABLE until re-assigned."
    warn "For demo purposes this is OK — restart pipeline to recover."
    echo -n "Continue? [y/N] "
    read -r ans
    [ "$ans" = "y" ] || [ "$ans" = "Y" ] || { log "Cancelled."; return; }

    docker compose stop kafka-2 kafka-3 2>/dev/null || true
    log "kafka-2 and kafka-3 stopped."

    log "Reverting Debezium to single broker..."
    curl -sf -X PUT "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/config" \
        -H "Content-Type: application/json" \
        -d "{
            \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",
            \"database.hostname\": \"cdc-mysql\",
            \"database.port\": \"3306\",
            \"database.user\": \"root\",
            \"database.password\": \"root\",
            \"database.server.id\": \"1\",
            \"database.include.list\": \"inventory\",
            \"topic.prefix\": \"inventory\",
            \"schema.history.internal.kafka.bootstrap.servers\": \"cdc-kafka:29092\",
            \"schema.history.internal.kafka.topic\": \"schema-changes.inventory\",
            \"include.schema.changes\": \"true\",
            \"decimal.handling.mode\": \"string\",
            \"tombstones.on.delete\": \"false\"
        }" > /dev/null 2>&1 && log "  Debezium reverted." || warn "  Could not revert Debezium"

    cmd_status
}

# ── bench ─────────────────────────────────────────────────────────────────
cmd_bench() {
    log "Sequential benchmark: 1 broker → 3 brokers → 1 broker"
    echo ""

    log "Phase 1: 1 broker"
    bash run_bench.sh quick 2>&1 | tail -5

    log "Adding brokers 2 + 3..."
    cmd_add
    cmd_rebalance
    sleep 30

    log "Phase 2: 3 brokers"
    bash run_bench.sh quick 2>&1 | tail -5

    log "Benchmark complete. Compare: python benchmark/compare_runs.py -n 6"
}

# ── dispatch ─────────────────────────────────────────────────────────────
CMD="${1:-status}"
case "$CMD" in
    status)    cmd_status   ;;
    add)       cmd_add      ;;
    rebalance) cmd_rebalance ;;
    remove)    cmd_remove   ;;
    bench)     cmd_bench    ;;
    *)
        echo "Usage: bash demo/kafka_scale.sh {status|add|rebalance|remove|bench}"
        exit 1
        ;;
esac
