#!/bin/bash
# setup_grafana.sh — Tự động setup Grafana sau mỗi lần bật máy
# Chạy: bash setup_grafana.sh
# Hoặc thêm vào cuối reset_pipeline.sh

GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASS="123456"
PROMETHEUS_URL="http://cdc-prometheus:9090"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RESET='\033[0m'

echo -e "${CYAN}Setting up Grafana...${RESET}"

# Đợi Grafana sẵn sàng
for i in $(seq 1 12); do
  if curl -s "${GRAFANA_URL}/api/health" | grep -q "ok"; then
    break
  fi
  echo -e "  Đợi Grafana... (${i}/12)"
  sleep 5
done

# Xóa datasource cũ nếu có
curl -s -X DELETE "${GRAFANA_URL}/api/datasources/name/Prometheus" \
  -u "${GRAFANA_USER}:${GRAFANA_PASS}" > /dev/null 2>&1

# Add datasource mới và lấy UID
DS_RESPONSE=$(curl -s -X POST "${GRAFANA_URL}/api/datasources" \
  -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Prometheus\",\"type\":\"prometheus\",\"url\":\"${PROMETHEUS_URL}\",\"access\":\"proxy\",\"isDefault\":true}")

DS_UID=$(echo "$DS_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('datasource',{}).get('uid',''))" 2>/dev/null)

if [[ -z "$DS_UID" ]]; then
  echo -e "${YELLOW}⚠ Không lấy được UID datasource — thử lấy từ existing${RESET}"
  DS_UID=$(curl -s "${GRAFANA_URL}/api/datasources/name/Prometheus" \
    -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('uid',''))" 2>/dev/null)
fi

echo -e "  Datasource UID: ${CYAN}${DS_UID}${RESET}"

# Build dashboard JSON với UID đúng
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARD_TEMPLATE="${SCRIPT_DIR}/grafana_dashboard_template.json"

if [[ ! -f "$DASHBOARD_TEMPLATE" ]]; then
  echo -e "${YELLOW}⚠ Không tìm thấy template — tạo dashboard cơ bản${RESET}"
  create_basic_dashboard "$DS_UID"
else
  import_dashboard "$DASHBOARD_TEMPLATE" "$DS_UID"
fi

echo -e "${GREEN}✓ Grafana setup xong!${RESET}"
echo -e "  URL: ${CYAN}${GRAFANA_URL}${RESET} (${GRAFANA_USER}/${GRAFANA_PASS})"

create_basic_dashboard() {
  local ds_uid="$1"
  local dashboard_json=$(cat << ENDJSON
{
  "uid": "cdc-auto",
  "title": "CDC Pipeline — Auto Dashboard",
  "refresh": "5s",
  "time": {"from": "now-30m", "to": "now"},
  "panels": [
    {
      "id": 1, "type": "stat", "title": "MySQL Customers",
      "gridPos": {"h":4,"w":4,"x":0,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_mysql_customers_total","refId":"A"}],
      "fieldConfig": {"defaults": {"color": {"mode":"thresholds"}, "thresholds": {"steps":[{"color":"green","value":null}]}}}
    },
    {
      "id": 2, "type": "stat", "title": "MongoDB Customers (synced)",
      "gridPos": {"h":4,"w":4,"x":4,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_mongo_customers_total","refId":"A"}],
      "fieldConfig": {"defaults": {"color": {"mode":"thresholds"}, "thresholds": {"steps":[{"color":"blue","value":null}]}}}
    },
    {
      "id": 3, "type": "stat", "title": "Sync Status",
      "gridPos": {"h":4,"w":4,"x":8,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_mysql_mongo_in_sync","refId":"A"}],
      "fieldConfig": {"defaults": {"mappings": [{"type":"value","options":{"0":{"text":"OUT OF SYNC","color":"red"},"1":{"text":"IN SYNC","color":"green"}}}], "color":{"mode":"thresholds"}, "thresholds":{"steps":[{"color":"red","value":null},{"color":"green","value":1}]}}}
    },
    {
      "id": 4, "type": "stat", "title": "MySQL Orders",
      "gridPos": {"h":4,"w":4,"x":12,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_mysql_orders_total","refId":"A"}],
      "fieldConfig": {"defaults": {"color": {"mode":"thresholds"}, "thresholds": {"steps":[{"color":"green","value":null}]}}}
    },
    {
      "id": 5, "type": "stat", "title": "Redis Revenue",
      "gridPos": {"h":4,"w":4,"x":16,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_redis_orders_revenue","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"short","color": {"mode":"thresholds"}, "thresholds": {"steps":[{"color":"orange","value":null}]}}}
    },
    {
      "id": 6, "type": "stat", "title": "Sync Lag (MySQL - MongoDB)",
      "description": "Phải luôn = 0",
      "gridPos": {"h":4,"w":4,"x":20,"y":0},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"cdc_mysql_customers_total - cdc_mongo_customers_total","refId":"A"}],
      "fieldConfig": {"defaults": {"color": {"mode":"thresholds"}, "thresholds": {"steps":[{"color":"green","value":null},{"color":"red","value":1}]}}}
    },
    {
      "id": 10, "type": "timeseries", "title": "LIVE: MySQL vs MongoDB (phải luôn bằng nhau)",
      "gridPos": {"h":8,"w":12,"x":0,"y":4},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [
        {"expr":"cdc_mysql_customers_total","legendFormat":"MySQL customers","refId":"A"},
        {"expr":"cdc_mongo_customers_total","legendFormat":"MongoDB customers (synced)","refId":"B"},
        {"expr":"cdc_mysql_orders_total","legendFormat":"MySQL orders","refId":"C"},
        {"expr":"cdc_mongo_orders_total","legendFormat":"MongoDB orders (synced)","refId":"D"}
      ]
    },
    {
      "id": 11, "type": "timeseries", "title": "Events/s — Rate (30s window)",
      "description": "Throughput thực tế của pipeline",
      "gridPos": {"h":8,"w":12,"x":12,"y":4},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [
        {"expr":"rate(cdc_mysql_customers_total[30s])","legendFormat":"MySQL insert rate","refId":"A"},
        {"expr":"rate(cdc_mongo_customers_total[30s])","legendFormat":"MongoDB sync rate","refId":"B"},
        {"expr":"rate(cdc_redis_customers_total[30s])","legendFormat":"Redis update rate","refId":"C"}
      ],
      "fieldConfig": {"defaults": {"unit":"reqps"}}
    },
    {
      "id": 20, "type": "gauge", "title": "E2E Latency p50 (benchmark)",
      "gridPos": {"h":5,"w":4,"x":0,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"2.024","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"s","min":0,"max":10,"thresholds":{"steps":[{"color":"green","value":null},{"color":"yellow","value":3},{"color":"red","value":6}]}}}
    },
    {
      "id": 21, "type": "gauge", "title": "E2E Latency p99 (benchmark)",
      "gridPos": {"h":5,"w":4,"x":4,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"4.049","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"s","min":0,"max":10,"thresholds":{"steps":[{"color":"green","value":null},{"color":"yellow","value":5},{"color":"red","value":8}]}}}
    },
    {
      "id": 22, "type": "gauge", "title": "Throughput E2E (benchmark)",
      "gridPos": {"h":5,"w":4,"x":8,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"35.48","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"reqps","min":0,"max":120,"thresholds":{"steps":[{"color":"red","value":null},{"color":"yellow","value":20},{"color":"green","value":35}]}}}
    },
    {
      "id": 23, "type": "gauge", "title": "Redis p99 Read Latency",
      "gridPos": {"h":5,"w":4,"x":12,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"0.531","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"ms","min":0,"max":5,"thresholds":{"steps":[{"color":"green","value":null},{"color":"yellow","value":1},{"color":"red","value":3}]}}}
    },
    {
      "id": 24, "type": "gauge", "title": "Batch Processing Time",
      "description": "Spark batch time — bottleneck chính",
      "gridPos": {"h":5,"w":4,"x":16,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"1200","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"ms","min":0,"max":3000,"thresholds":{"steps":[{"color":"green","value":null},{"color":"yellow","value":1000},{"color":"red","value":1800}]}}}
    },
    {
      "id": 25, "type": "gauge", "title": "Min Safe Trigger",
      "description": "= batch_time x 1.5",
      "gridPos": {"h":5,"w":4,"x":20,"y":12},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "targets": [{"expr":"2000","refId":"A"}],
      "fieldConfig": {"defaults": {"unit":"ms","min":0,"max":5000,"thresholds":{"steps":[{"color":"green","value":null}]}}}
    },
    {
      "id": 30, "type": "bargauge", "title": "Bottleneck Analysis — Partition Scaling",
      "description": "1 vs 3 partition — throughput gần như không đổi → bottleneck là Spark trigger",
      "gridPos": {"h":5,"w":12,"x":0,"y":17},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "options": {"orientation":"horizontal","displayMode":"gradient"},
      "targets": [
        {"expr":"35.33","legendFormat":"1 partition (đo thực tế)","refId":"A"},
        {"expr":"35.48","legendFormat":"3 partitions (đo thực tế)","refId":"B"}
      ],
      "fieldConfig": {"defaults": {"unit":"reqps","min":0,"max":60,"thresholds":{"steps":[{"color":"blue","value":null}]}}}
    },
    {
      "id": 31, "type": "bargauge", "title": "Trigger Experiment — Latency p50",
      "description": "Trigger 2s là tối thiểu an toàn với batch ~1200ms",
      "gridPos": {"h":5,"w":12,"x":12,"y":17},
      "datasource": {"type":"prometheus","uid":"${ds_uid}"},
      "options": {"orientation":"horizontal","displayMode":"gradient"},
      "targets": [
        {"expr":"4.038","legendFormat":"trigger=5s (baseline)","refId":"A"},
        {"expr":"2.830","legendFormat":"trigger=2s (current)","refId":"B"},
        {"expr":"999","legendFormat":"trigger=1s (FAIL - falling behind)","refId":"C"},
        {"expr":"999","legendFormat":"trigger=500ms (FAIL - falling behind)","refId":"D"}
      ],
      "fieldConfig": {"defaults": {"unit":"s","min":0,"max":10,"thresholds":{"steps":[{"color":"green","value":null},{"color":"yellow","value":3},{"color":"red","value":5}]}}}
    },
    {
      "id": 40, "type": "text", "title": "Industry Comparison",
      "gridPos": {"h":8,"w":24,"x":0,"y":22},
      "options": {
        "mode": "markdown",
        "content": "## So sánh với ngành\n\n| Metric | **Pipeline này** | Debezium docs | Databricks RTM | Netflix |\n|---|---|---|---|---|\n| **E2E p50** | **2.02s** | ~ms (DB→Kafka only) | <200ms (cloud) | seconds |\n| **Throughput** | **35 ev/s** | ~7,000 ev/s (connector) | 100K+ ev/s | 100M ev/s |\n| **Redis p99** | **<1ms** | N/A | N/A | <100μs |\n| **Infra** | 1 laptop · Docker | 2× EC2 | 7× i3.2xlarge | K8s · multi-DC |\n| **Bottleneck** | **Spark trigger 2s** | DB write cap | N/A | N/A |\n\n**Key insight:** Tăng Kafka partition 1→3: throughput 35.33 vs 35.48 ev/s (+0.4%) → bottleneck là Spark trigger, không phải Kafka. Trigger 500ms và 1s đều FAIL (batch ~1200ms > trigger interval)."
      }
    }
  ]
}
ENDJSON
)

  echo "$dashboard_json" | python3 -c "
import sys, json
dash = json.load(sys.stdin)
payload = json.dumps({'dashboard': dash, 'overwrite': True, 'folderId': 0})
print(payload)
" | curl -s -X POST "${GRAFANA_URL}/api/dashboards/db" \
    -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
    -H "Content-Type: application/json" \
    -d @- | python3 -c "import sys,json; d=json.load(sys.stdin); print('Dashboard:', d.get('status'), d.get('url',''))"
}

import_dashboard() {
  local template="$1"
  local ds_uid="$2"
  sed "s/DATASOURCE_UID/${ds_uid}/g" "$template" | \
    python3 -c "
import sys, json
dash = json.load(sys.stdin)
payload = json.dumps({'dashboard': dash, 'overwrite': True, 'folderId': 0})
print(payload)
" | curl -s -X POST "${GRAFANA_URL}/api/dashboards/db" \
      -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
      -H "Content-Type: application/json" \
      -d @- | python3 -c "import sys,json; d=json.load(sys.stdin); print('Dashboard:', d.get('status'), d.get('url',''))"
}

# Gọi function tạo dashboard
create_basic_dashboard "$DS_UID"

# Import dashboard qua API (chắc chắn hơn provisioning reload)
echo "Importing dashboard..."
curl -s -X POST http://admin:admin@localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d "{\"dashboard\": $(cat ~/cdc-pipeline/monitoring/grafana/dashboards/cdc_dashboard.json), \"overwrite\": true, \"folderId\": 0}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('Dashboard:', d.get('status'), d.get('url',''))"
