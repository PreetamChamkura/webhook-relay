#!/usr/bin/env bash
# Subscriber-outage scenario: take the receiver down during load, watch the DLQ
# grow, bring it back, replay, and confirm every event arrives once.
set -euo pipefail
BASE=${BASE_URL:-http://localhost:8080}
RCV=${RECEIVER_ADMIN:-http://localhost:8090}

SUB=$(curl -s -XPOST "$BASE/v1/subscriptions" -H 'content-type: application/json' \
  -d '{"url":"http://receiver:8090/hook","event_types":["outage.test"]}' | jq -r .id)
echo "subscription $SUB"

curl -s -XPOST "$RCV/toggle"; echo "  <- receiver down"
for i in $(seq 1 500); do
  curl -s -o /dev/null -XPOST "$BASE/v1/events" -H 'content-type: application/json' \
    -d "{\"event_type\":\"outage.test\",\"payload\":{\"i\":$i}}"
done
echo "published 500 while down; waiting for retries to exhaust..."
sleep 60
curl -s "$BASE/metrics" | grep -E '^webhook_dead_lettered_total'

curl -s -XPOST "$RCV/toggle"; echo "  <- receiver up"
curl -s -XPOST "$BASE/v1/subscriptions/$SUB/replay"; echo
sleep 10
curl -s "$RCV/metrics" | grep -E '^receiver_webhooks_total'
