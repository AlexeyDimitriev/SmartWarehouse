.PHONY: build
build:
	go build -o bin/consumer ./cmd/consumer

.PHONY: run
run:
	go run ./cmd/consumer

.PHONY: test
test:
	go test ./...

.PHONY: test-cycle
test-cycle:
	@echo "Running test-cycle script..."
	./scripts/test-cycle.sh

.PHONY: docker-up
docker-up:
	docker-compose up -d

.PHONY: docker-down
docker-down:
	docker-compose down

.PHONY: create-topic
create-topic:
	docker exec kafka kafka-topics --create \
		--topic warehouse-events \
		--bootstrap-server localhost:9092 \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists

.PHONY: produce-test-event
produce-test-event:
	echo '{"event_id":"test-001","event_type":"PRODUCT_RECEIVED","timestamp":"2026-05-08T10:00:00Z","payload":{"product_id":"SKU-001","quantity":100,"zone_id":"ZONE-51"}}' | \
	docker exec -i kafka kafka-console-producer \
		--topic warehouse-events \
		--bootstrap-server localhost:9092