#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$DIR/connector.json"
