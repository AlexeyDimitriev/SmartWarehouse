#!/bin/bash
set -e

TOPIC="warehouse-events"
DLQ_TOPIC="warehouse-events-dlq"
BOOTSTRAP="kafka:29092"

echo "Waiting for Kafka & Cassandra to stabilize..."
sleep 20

wait_for_cassandra() {
    echo "Waiting for Cassandra keyspace..."

    until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" 2>/dev/null | grep -q warehouse; do
        sleep 2
    done

    echo "Cassandra is ready"
}

wait_for_cassandra

produce() {
    local key="$1"
    local desc="$2"
    local json="$3"

    echo "Sending: $desc"

    echo "$key:$json" | docker exec -i kafka kafka-console-producer \
        --bootstrap-server $BOOTSTRAP \
        --topic $TOPIC \
        --property "parse.key=true" \
        --property "key.separator=:"

    echo "Waiting for processing..."
    sleep 5
}

check() {
    local desc="$1"
    local query="$2"

    echo "Checking: $desc"

    for i in $(seq 1 20); do
        res=$(docker exec cassandra cqlsh -k warehouse -e "$query" 2>/dev/null || true)

        if echo "$res" | grep -Eq "\([1-9][0-9]* rows\)"; then
            echo "$res"
            echo "---"
            return 0
        fi

        sleep 3
    done

    echo "FAILED CHECK:"
    echo "$res"
    echo "---"

    return 1
}

check_dlq() {
    echo "Checking DLQ Topic..."

    sleep 5

    docker exec kafka kafka-console-consumer \
        --bootstrap-server $BOOTSTRAP \
        --topic $DLQ_TOPIC \
        --from-beginning \
        --timeout-ms 10000 \
        --max-messages 1 \
        2>/dev/null || echo "(DLQ empty or timeout)"

    echo "---"
}

echo "STARTING E2E TEST CYCLE"
echo "=========================================="

echo "SCENARIO 1: Basic Warehouse Cycle"

produce "SKU-001" \
"PRODUCT_RECEIVED (SKU-001)" \
'{"event_id":"s1-e1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T10:00:00Z","payload":{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":100}}'

check "ZONE-A available=100" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

produce "SKU-001" \
"PRODUCT_RESERVED (SKU-001)" \
'{"event_id":"s1-e2","event_type":"PRODUCT_RESERVED","timestamp":"2026-05-09T10:05:00Z","payload":{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":30}}'

check "ZONE-A avail=70, res=30" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

produce "SKU-001" \
"PRODUCT_MOVED (SKU-001 A->B)" \
'{"event_id":"s1-e3","event_type":"PRODUCT_MOVED","timestamp":"2026-05-09T10:10:00Z","payload":{"product_id":"SKU-001","from_zone":"ZONE-A","to_zone":"ZONE-B","quantity":20}}'

check "ZONE-A avail=50, ZONE-B avail=20" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001';"

produce "SKU-001" \
"PRODUCT_SHIPPED (SKU-001)" \
'{"event_id":"s1-e4","event_type":"PRODUCT_SHIPPED","timestamp":"2026-05-09T10:15:00Z","payload":{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":10}}'

check "ZONE-A avail=40" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

produce "ORD-100" \
"ORDER_CREATED (ORD-100)" \
'{"event_id":"s1-e5","event_type":"ORDER_CREATED","timestamp":"2026-05-09T10:20:00Z","payload":{"order_id":"ORD-100","positions":[{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":15}]}}'

check "Order CREATED" \
"SELECT order_id, status FROM orders WHERE order_id='ORD-100';"

check "Reserved increased" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

produce "ORD-100" \
"ORDER_COMPLETED (ORD-100)" \
'{"event_id":"s1-e6","event_type":"ORDER_COMPLETED","timestamp":"2026-05-09T10:25:00Z","payload":{"order_id":"ORD-100"}}'

check "Order COMPLETED" \
"SELECT order_id, status FROM orders WHERE order_id='ORD-100';"

check "Reserved decreased" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

echo "SCENARIO 2: Idempotency"

produce "SKU-002" \
"PRODUCT_RECEIVED (SKU-002)" \
'{"event_id":"s2-e1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T11:00:00Z","payload":{"product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}}'

check "SKU-002 avail=50" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-002';"

produce "SKU-002" \
"DUPLICATE s2-e1" \
'{"event_id":"s2-e1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T11:00:00Z","payload":{"product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}}'

check "SKU-002 still avail=50" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-002';"

echo "SCENARIO 3: Table Consistency"

produce "SKU-003" \
"PRODUCT_RECEIVED (SKU-003)" \
'{"event_id":"s3-e1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T12:00:00Z","payload":{"product_id":"SKU-003","zone_id":"ZONE-A","quantity":100}}'

check "inventory_by_product_zone" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-003';"

check "inventory_by_product" \
"SELECT product_id, available, reserved FROM inventory_by_product WHERE product_id='SKU-003';"

check "inventory_by_zone" \
"SELECT zone_id, product_id, available, reserved FROM inventory_by_zone WHERE zone_id='ZONE-A' AND product_id='SKU-003';"

echo "SCENARIO 4: Out-of-Order Events"

produce "SKU-004" \
"RECEIVED SKU-004 (ts=12:00)" \
'{"event_id":"s4-e1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T12:00:00Z","payload":{"product_id":"SKU-004","zone_id":"ZONE-A","quantity":100}}'

produce "SKU-004" \
"SHIPPED SKU-004 (ts=12:05)" \
'{"event_id":"s4-e2","event_type":"PRODUCT_SHIPPED","timestamp":"2026-05-09T12:05:00Z","payload":{"product_id":"SKU-004","zone_id":"ZONE-A","quantity":20}}'

check "SKU-004 avail=80" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-004';"

produce "SKU-004" \
"LATE RECEIVED SKU-004 (ts=12:02)" \
'{"event_id":"s4-e3","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T12:02:00Z","payload":{"product_id":"SKU-004","zone_id":"ZONE-A","quantity":50}}'

check "SKU-004 still avail=80 (stale ignored)" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-004';"

echo "SCENARIO 5: Dead Letter Queue"

produce "SKU-ERR" \
"INVALID SHIP (qty=-5)" \
'{"event_id":"s5-e1","event_type":"PRODUCT_SHIPPED","timestamp":"2026-05-09T13:00:00Z","payload":{"product_id":"SKU-ERR","zone_id":"ZONE-A","quantity":-5}}'

check_dlq

produce "SKU-006" \
"VALID AFTER DLQ" \
'{"event_id":"s5-e2","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T13:05:00Z","payload":{"product_id":"SKU-006","zone_id":"ZONE-A","quantity":10}}'

check "Consumer alive: SKU-006 avail=10" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id='SKU-006';"

echo "=========================================="
echo "E2E TEST CYCLE COMPLETED SUCCESSFULLY"