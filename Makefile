# VikDB Makefile
# Build, run (basic + 3-node cluster), and dev with air

BINARY := vikdb
PKG := ./cmd/vikdb

# Basic mode flags (single node, from README)
ADDR := :8080
MEMTABLE_MAX := 104857600
WAL_PATH := ./data/wal
SSTABLE_DIR := ./data/sstables

# Cluster replication addresses (all nodes, for -cluster)
CLUSTER_ADDRS := localhost:8081,localhost:8083,localhost:8085

# Benchmark (run VikDB first, e.g. make run)
BENCH_ADDR       ?= localhost:8080
BENCH_RPS        ?= 500
BENCH_DURATION   ?= 10s
BENCH_CONCURRENCY ?= 10

.PHONY: build install run run-node1 run-node2 run-node3 run-cluster dev test clean help bench bench-put bench-read bench-range bench-batch bench-delete bench-mixed

# Install all project dependencies and dev tools (air)
install:
	go mod download
	go mod verify
	go install github.com/air-verse/air@latest
	@echo "Dependencies and air installed. Ensure $(shell go env GOPATH)/bin is in your PATH for air."

# Build the binary
build:
	go build -o $(BINARY) $(PKG)

# Run in basic (single-node) mode
run: build
	./$(BINARY) -addr $(ADDR) \
		-memtable-max-size $(MEMTABLE_MAX) \
		-wal-path $(WAL_PATH) \
		-sstable-dir $(SSTABLE_DIR)

# Run node 1 of 3-node cluster (use in terminal 1)
run-node1: build
	./$(BINARY) -addr :8080 -replication-port 8081 \
		-node-id node1 -cluster "$(CLUSTER_ADDRS)" \
		-wal-path ./data/node1/wal -sstable-dir ./data/node1/sstables

# Run node 2 of 3-node cluster (use in terminal 2)
run-node2: build
	./$(BINARY) -addr :8082 -replication-port 8083 \
		-node-id node2 -cluster "$(CLUSTER_ADDRS)" \
		-wal-path ./data/node2/wal -sstable-dir ./data/node2/sstables

# Run node 3 of 3-node cluster (use in terminal 3)
run-node3: build
	./$(BINARY) -addr :8084 -replication-port 8085 \
		-node-id node3 -cluster "$(CLUSTER_ADDRS)" \
		-wal-path ./data/node3/wal -sstable-dir ./data/node3/sstables

# Run 3-node cluster: open 3 terminals and run make run-node1, make run-node2, make run-node3
run-cluster:
	@echo "Run the 3-node cluster in 3 separate terminals:"
	@echo "  Terminal 1: make run-node1"
	@echo "  Terminal 2: make run-node2"
	@echo "  Terminal 3: make run-node3"

# Dev: live reload with air (basic mode). Install air: go install github.com/air-verse/air@latest
dev:
	air

# Run tests
test:
	go test ./...

# Run tests with verbose output
test-v:
	go test -v ./...

# Benchmark (VikDB must be running). Override: make bench BENCH_RPS=1000 BENCH_DURATION=30s
bench:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY)

# Per-operation benchmarks (100% single op type)
bench-put:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 100 -read 0 -range 0 -batch 0 -delete 0
bench-read:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 0 -read 100 -range 0 -batch 0 -delete 0
bench-range:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 0 -read 0 -range 100 -batch 0 -delete 0
bench-batch:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 0 -read 0 -range 0 -batch 100 -delete 0
bench-delete:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 0 -read 0 -range 0 -batch 0 -delete 100

# Mixed workload (put 40%, read 30%, range 10%, batch 10%, delete 10%). Load data first with bench-put.
bench-mixed:
	go run ./cmd/vikdb-bench -addr $(BENCH_ADDR) -rps $(BENCH_RPS) -duration $(BENCH_DURATION) -concurrency $(BENCH_CONCURRENCY) -put 40 -read 30 -range 10 -batch 10 -delete 10

# Remove built binary and tmp dir used by air
clean:
	rm -f $(BINARY)
	rm -rf ./tmp

help:
	@echo "VikDB targets:"
	@echo "  make install   - install project deps (go mod) and air"
	@echo "  make build     - build ./vikdb"
	@echo "  make run        - run single-node (basic) mode"
	@echo "  make run-node1  - run cluster node 1 (API :8080, RPC :8081)"
	@echo "  make run-node2  - run cluster node 2 (API :8082, RPC :8083)"
	@echo "  make run-node3  - run cluster node 3 (API :8084, RPC :8085)"
	@echo "  make run-cluster - print how to run 3-node cluster"
	@echo "  make dev        - run basic mode with air (live reload)"
	@echo "  make test       - run tests"
	@echo "  make bench      - run benchmark (default: put-only, 500 RPS, 10s). Start VikDB first (make run)."
	@echo "  make bench-put bench-read bench-range bench-batch bench-delete - 100%% single op"
	@echo "  make bench-mixed - put 40%%, read 30%%, range 10%%, batch 10%%, delete 10%%"
	@echo "  Override: make bench BENCH_RPS=1000 BENCH_DURATION=30s BENCH_ADDR=localhost:8080"
	@echo "  make clean      - remove binary and tmp"
