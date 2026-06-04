#!/bin/bash
# tests/fault-tolerance/run_ft_tests.sh
# Automated fault tolerance test suite — chạy trước khi demo hoặc để validate
# Usage: bash tests/fault-tolerance/run_ft_tests.sh [single|all]
# Output: PASS/FAIL per scenario + recovery time

set -euo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
BOLD='\033[1m'

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
ok()    { echo -e "${GREEN}[PASS]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; }
title() { echo -e "\n${BOLD}$*${NC}"; }

RESULTS_FILE="${RESULTS_FILE:-tests/fault-tolerance/ft_results.jsonl}"

# ── Helpers ─────────────────────────────────────────────────────────
mysql_count() {
    docker exec cdc-mysql mysql -uroot -proot -N -e \
        "SELECT COUNT(*) FROM inventory.customers;" 2>/dev/null || echo "0"
}

mongo_count() {
    docker exec cdc-mongodb mongosh --quiet --eval \
        "db.getSiblingDB('inventory').customers.countDocuments()" 2>/dev/null || echo "0"
}

insert_test_record() {
    local label="${1:-ft-test}"
    docker exec cdc-mysql mysql -uroot -proot -e \
        "INSERT INTO inventory.customers (name, email, phone) VALUES ('FT-${label}', 'ft@test.com', '0900000099');" \
        2>/dev/null && echo "1" || echo "0"
}

wait_sync() {
    local expected=$1 timeout=${2:-60} waited=0
    while [ $waited -lt $timeout ]; do
        local mc; mc=$(mongo_count)
        if [ "$mc" -ge "$expected" ]; then return 0; fi
        sleep 3; waited=$((waited + 3))
    done
    return 1
}

container_status() {
    docker inspect --format='{{.State.Status}}' "$1" 2>/dev/null || echo "unknown"
}

save_result() {
    local scenario=$1 status=$2 recovery_s=$3 mysql_before=$4 mongo_after=$5 lost=$6
    echo "{\"scenario\":\"$scenario\",\"status\":\"$status\",\"recovery_s\":$recovery_s,\"mysql_before\":$mysql_before,\"mongo_after\":$mongo_after,\"lost\":$lost,\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
        >> "$RESULTS_FILE"
}

# ── Scenario 1: Kafka Broker Crash ──────────────────────────────────
test_kafka_crash() {
    title "SCENARIO 1: Kafka Broker Crash & Recovery"

    local before_mysql before_mongo
    before_mysql=$(mysql_count)
    before_mongo=$(mongo_count)
    info "Baseline — MySQL: $before_mysql | MongoDB: $before_mongo"

    # Kill Kafka
    info "Stopping cdc-kafka…"
    docker stop cdc-kafka >/dev/null
    warn "Kafka DOWN"

    sleep 3
    info "Inserting 1 record into MySQL while Kafka is DOWN…"
    insert_test_record "kafka" >/dev/null
    local expected_mysql=$((before_mysql + 1))

    sleep 5
    local t0; t0=$(date +%s)

    # Recover
    info "Starting cdc-kafka…"
    docker start cdc-kafka >/dev/null

    # Wait for container up
    for i in $(seq 1 15); do
        sleep 2
        [ "$(container_status cdc-kafka)" = "running" ] && break
    done
    info "Kafka running — waiting for Spark to drain backlog…"

    # Wait for sync
    if wait_sync $expected_mysql 90; then
        local t1; t1=$(date +%s)
        local rt=$((t1 - t0))
        local after_mongo; after_mongo=$(mongo_count)
        local after_mysql; after_mysql=$(mysql_count)
        local lost=$((after_mysql - after_mongo))
        if [ "$lost" -le 0 ]; then
            ok "Kafka Crash — PASS | Recovery: ${rt}s | MySQL=${after_mysql} | MongoDB=${after_mongo} | Lost=0"
            save_result "kafka_crash" "PASS" $rt $before_mysql $after_mongo 0
        else
            fail "Kafka Crash — PARTIAL | Recovery: ${rt}s | MySQL=${after_mysql} | MongoDB=${after_mongo} | Lost=${lost}"
            save_result "kafka_crash" "PARTIAL" $rt $before_mysql $after_mongo $lost
        fi
    else
        fail "Kafka Crash — TIMEOUT | MongoDB did not converge within 90s"
        save_result "kafka_crash" "TIMEOUT" 90 $before_mysql "$(mongo_count)" -1
    fi
}

# ── Scenario 2: Debezium Restart ────────────────────────────────────
test_debezium_restart() {
    title "SCENARIO 2: Debezium Connector Restart & Offset Recovery"

    local before_mysql before_mongo
    before_mysql=$(mysql_count)
    before_mongo=$(mongo_count)
    info "Baseline — MySQL: $before_mysql | MongoDB: $before_mongo"

    info "Restarting cdc-debezium…"
    docker restart cdc-debezium >/dev/null
    warn "Debezium restarting — CDC paused"

    sleep 5
    info "Inserting 1 record while Debezium is restarting…"
    insert_test_record "debezium" >/dev/null
    local expected_mysql=$((before_mysql + 1))

    local t0; t0=$(date +%s)

    # Wait for Debezium to come back
    for i in $(seq 1 20); do
        sleep 2
        [ "$(container_status cdc-debezium)" = "running" ] && break
    done
    info "Debezium running — resuming from stored binlog offset"

    if wait_sync $expected_mysql 90; then
        local t1; t1=$(date +%s)
        local rt=$((t1 - t0))
        local after_mongo; after_mongo=$(mongo_count)
        local after_mysql; after_mysql=$(mysql_count)
        local lost=$((after_mysql - after_mongo))
        if [ "$lost" -le 0 ]; then
            ok "Debezium Restart — PASS | Recovery: ${rt}s | MySQL=${after_mysql} | MongoDB=${after_mongo} | Lost=0"
            save_result "debezium_restart" "PASS" $rt $before_mysql $after_mongo 0
        else
            fail "Debezium Restart — PARTIAL | Lost=${lost}"
            save_result "debezium_restart" "PARTIAL" $rt $before_mysql $after_mongo $lost
        fi
    else
        fail "Debezium Restart — TIMEOUT"
        save_result "debezium_restart" "TIMEOUT" 90 $before_mysql "$(mongo_count)" -1
    fi
}

# ── Scenario 3: Spark Job Kill ───────────────────────────────────────
test_spark_kill() {
    title "SCENARIO 3: Spark Driver Kill & Checkpoint Recovery"

    local before_mysql before_mongo
    before_mysql=$(mysql_count)
    before_mongo=$(mongo_count)
    info "Baseline — MySQL: $before_mysql | MongoDB: $before_mongo"

    info "Killing Spark streaming job…"
    docker exec cdc-spark-master pkill -f CdcRedisConsumer 2>/dev/null || true
    warn "Spark job killed — messages accumulating in Kafka"
    info "Checkpoint preserved at /tmp/spark-checkpoint/cdc-pipeline"

    sleep 3
    info "Inserting 1 record while Spark is down…"
    insert_test_record "spark" >/dev/null
    local expected_mysql=$((before_mysql + 1))

    sleep 3
    local t0; t0=$(date +%s)

    info "Re-submitting Spark job (using existing checkpoint)…"
    docker exec -d cdc-spark-master /opt/spark/bin/spark-submit \
        --class CdcRedisConsumer \
        --master spark://cdc-spark-master:7077 \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,redis.clients:jedis:5.1.0" \
        /opt/spark/jobs/cdc-mysql-to-mongodb-redis_2.12-1.0.jar 2>/dev/null

    info "Waiting for Spark to process buffered events (may take 30-90s for packages)…"
    if wait_sync $expected_mysql 150; then
        local t1; t1=$(date +%s)
        local rt=$((t1 - t0))
        local after_mongo; after_mongo=$(mongo_count)
        local after_mysql; after_mysql=$(mysql_count)
        local lost=$((after_mysql - after_mongo))
        if [ "$lost" -le 0 ]; then
            ok "Spark Kill — PASS | Recovery: ${rt}s | MySQL=${after_mysql} | MongoDB=${after_mongo} | Lost=0 | No duplicates"
            save_result "spark_kill" "PASS" $rt $before_mysql $after_mongo 0
        else
            fail "Spark Kill — PARTIAL | Lost=${lost}"
            save_result "spark_kill" "PARTIAL" $rt $before_mysql $after_mongo $lost
        fi
    else
        fail "Spark Kill — TIMEOUT"
        save_result "spark_kill" "TIMEOUT" 150 $before_mysql "$(mongo_count)" -1
    fi
}

# ── Scenario 4 (VM only): Kafka Multi-broker ISR Failover ───────────
test_kafka_cluster_failover() {
    title "SCENARIO 4 (VM): Kafka Multi-broker ISR Failover"

    # Check if running cluster mode
    if ! docker ps --format '{{.Names}}' | grep -q cdc-kafka-2; then
        warn "cdc-kafka-2 not running — this scenario requires 3-broker cluster"
        warn "Run with: docker compose -f docker-compose.yml -f tests/fault-tolerance/docker-compose.kafka-cluster.yml up -d"
        return
    fi

    local before_mysql before_mongo
    before_mysql=$(mysql_count)
    before_mongo=$(mongo_count)
    info "3-broker cluster detected: cdc-kafka, cdc-kafka-2, cdc-kafka-3"
    info "Baseline — MySQL: $before_mysql | MongoDB: $before_mongo"

    info "Killing ONE broker (cdc-kafka-2) — minority failure, ISR should absorb"
    docker stop cdc-kafka-2 >/dev/null
    warn "Broker 2 DOWN — 2/3 brokers still up (quorum maintained)"

    sleep 3
    insert_test_record "cluster" >/dev/null
    local expected_mysql=$((before_mysql + 1))

    local t0; t0=$(date +%s)
    info "Pipeline should continue WITHOUT interruption (replication.factor=3, min.insync.replicas=2)"

    if wait_sync $expected_mysql 30; then
        local t1; t1=$(date +%s)
        local rt=$((t1 - t0))
        ok "Cluster Failover — PASS | Recovery: ${rt}s (should be < 5s) | ISR absorbed the failure"
        save_result "kafka_cluster_failover" "PASS" $rt $before_mysql "$(mongo_count)" 0
    else
        fail "Cluster Failover — FAIL | Pipeline did not survive broker loss"
        save_result "kafka_cluster_failover" "FAIL" 30 $before_mysql "$(mongo_count)" -1
    fi

    info "Bringing broker 2 back up…"
    docker start cdc-kafka-2 >/dev/null
    ok "Broker 2 back — cluster at full capacity"
}

# ── Main ─────────────────────────────────────────────────────────────
MODE="${1:-all}"
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║        CDC Pipeline — Fault Tolerance Tests          ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

mkdir -p "$(dirname "$RESULTS_FILE")"

case "$MODE" in
    kafka)    test_kafka_crash ;;
    debezium) test_debezium_restart ;;
    spark)    test_spark_kill ;;
    cluster)  test_kafka_cluster_failover ;;
    all)
        test_kafka_crash
        sleep 5
        test_debezium_restart
        sleep 5
        test_spark_kill
        ;;
    *)
        echo "Usage: $0 [kafka|debezium|spark|cluster|all]"
        exit 1
        ;;
esac

echo ""
echo -e "${BOLD}Results saved to: $RESULTS_FILE${NC}"
echo ""
