#!/bin/bash
set -e

TOPIC="warehouse-events"
BOOTSTRAP="kafka:29092"

produce() {
    local desc="$1"
    local json="$2"
    echo "Sending Event: $desc"
    echo "$json" | docker exec -i kafka kafka-console-producer --bootstrap-server $BOOTSTRAP --topic $TOPIC
    echo "Waiting for processing..."
    sleep 30
}

check() {
    local desc="$1"
    local query="$2"
    echo "$desc"
    docker exec cassandra cqlsh -k warehouse -e "$query"
    echo ""
}

echo "STARTING FULL WAREHOUSE TEST CYCLE"
echo "=========================================="

echo "STEP 1: Creating Inventory"
produce "PRODUCT_RECEIVED (SKU-001)" \
'{"event_id":"evt-1","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T10:00:00Z","payload":{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":100}}'

produce "PRODUCT_RECEIVED (SKU-002)" \
'{"event_id":"evt-2","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-09T10:05:00Z","payload":{"product_id":"SKU-002","zone_id":"ZONE-B","quantity":50}}'

check "Current Inventory" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id IN ('SKU-001', 'SKU-002');"

echo "STEP 2: Creating Order"
produce "ORDER_CREATED (ORD-100)" \
'{"event_id":"evt-3","event_type":"ORDER_CREATED","timestamp":"2026-05-09T11:00:00Z","payload":{"order_id":"ORD-100","positions":[{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":20},{"product_id":"SKU-002","zone_id":"ZONE-B","quantity":10}]}}'

check "Order Status" "SELECT * FROM orders WHERE order_id = 'ORD-100';"
check "Inventory after Reservation" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id IN ('SKU-001', 'SKU-002');"

echo "STEP 3: Completing Order"
produce "ORDER_COMPLETED (ORD-100)" \
'{"event_id":"evt-4","event_type":"ORDER_COMPLETED","timestamp":"2026-05-09T12:00:00Z","payload":{"order_id":"ORD-100"}}'

check "Order Status (COMPLETED)" "SELECT * FROM orders WHERE order_id = 'ORD-100';"
check "Final Inventory" \
"SELECT product_id, zone_id, available, reserved FROM inventory_by_product_zone WHERE product_id IN ('SKU-001', 'SKU-002');"

echo "TEST CYCLE COMPLETED"
