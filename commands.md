# Ware_Go Commands Reference Guide

A centralized reference of all terminal commands used across the **AegisWMS (Ware_Go)** project, what each command does, why it is used, and commands we use in upcoming phases.

---

## 1. Environment & Setup Concepts

### What is `%USERPROFILE%` and Why Use It? (Windows)
* `%USERPROFILE%` is a system environment variable pointing to your user directory (e.g., `C:\Users\yash8`).
* When you run `go install <tool>@latest`, Go compiles the executable into `%USERPROFILE%\go\bin\`.
* Running `%USERPROFILE%\go\bin\sqlc` or adding that directory to your PATH lets you invoke tools directly.

### What is `.git/index.lock`?
* A lock file Git creates during any staging or commit operation (`git add`, `git commit`).
* If a command crashes or is abruptly killed, `index.lock` remains behind as an orphan, blocking future Git commands.
* Fix: `del .git\index.lock` safely clears the stale lock.

---

## 2. All Commands Used in This Project

### A. Go Build & Code Quality

* **`go build ./...`**
  * **What it does**: Compiles every package in the project recursively (`./...`).
  * **Why use it**: Quickest way to verify that all syntax, imports, types, and function signatures across the entire project compile with zero errors, without producing a binary on disk.

* **`go vet ./...`**
  * **What it does**: Go’s built-in static analysis tool (linter).
  * **Why use it**: Catches subtle bugs that compile fine but fail at runtime (e.g., wrong `Printf` format verbs, variable shadowing, mutexes copied by value).

* **`go mod tidy`**
  * **What it does**: Scans your source code, downloads any imported dependencies into your Go module cache, removes unused dependencies, and updates `go.mod` and `go.sum`.

---

### B. Testing & Concurrency Verification

* **`go test -v ./pkg/geo/...`**
  * **What it does**: Runs unit tests for the Haversine distance package.
  * **`-v`**: Verbose mode (prints individual test names and `=== RUN`, `--- PASS` logs).

* **`go test -v ./test/integration/...`**
  * **What it does**: Runs integration tests against the live PostgreSQL container.
  * **What it verifies**: Database schema constraints (e.g. `CHECK (available_qty >= 0)` rejecting negative stock with `SQLSTATE 23514`).

* **`go test -v -run <TestName> ./test/integration/...`**
  * **What it does**: Runs only tests matching the regex `<TestName>` (e.g. `TestConcurrentReservationZeroOversell`).
  * **Example**:
    ```bash
    go test -v -run TestConcurrentReservationZeroOversell ./test/integration/...
    ```

* **`go test -v -race -run <TestName> ./test/integration/...`**
  * **What it does**: Runs tests with Google's **ThreadSanitizer (Race Detector)** compiled into the binary.
  * **Why use it**: Mathematically proves that 1,000 concurrent goroutines accessing shared state produce **zero data races** (no unsynchronized memory reads/writes).
  * **Why it takes longer**: The compiler injects surveillance code around every memory access, adding 2x–5x CPU and 5x–10x memory overhead during the test.

* **`go test -bench=. ./test/integration/...`** *(Week 4)*
  * **What it does**: Runs benchmark functions (`BenchmarkXxx`) to measure operations per second and latency curves across locking strategies.

---

### C. Docker & Infrastructure (Postgres 16 + Redis 7)

* **`docker compose up -d`**
  * **What it does**: Reads `docker-compose.yml`, pulls images (`postgres:16-alpine`, `redis:7-alpine`), and starts containers in detached (background) mode.

* **`docker compose up -d --force-recreate`** *(Week 5)*
  * **What it does**: Forces Docker to destroy and recreate all containers even if Docker thinks the configuration has not changed. Guarantees new flags, ports, and environment variables are active.

* **`docker logs Ware_Go_redpanda --tail 20`** *(Week 5)*
  * **What it does**: Inspects the most recent 20 lines of Redpanda / Kafka broker startup and partition cluster health logs.

* **`docker compose ps`**
  * **What it does**: Displays the status and healthcheck results (`Up (healthy)`) of all services.

* **`docker compose down`**
  * **What it does**: Stops and removes running containers without deleting database data.

* **`docker compose down -v`**
  * **What it does**: Stops containers **and deletes all named volumes** (`postgres_data`, `redis_data`). Use this to completely reset the database to a fresh state.

---

### D. Database Migration & Direct Inspection

* **`docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations\001_init.sql`**
  * **What it does**: Pipes the local SQL migration file into `psql` running inside the PostgreSQL container to initialize tables, constraints, and indexes.

* **`docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations\002_outbox.sql`** *(Week 5)*
  * **What it does**: Applies the Transactional Outbox table schema and partial index to PostgreSQL.

* **`docker exec -it Ware_Go_postgres psql -U postgres -d ware_go -c "\dt"`**
  * **What it does**: Opens an interactive terminal session inside the container and runs the `psql` command to list all tables.

* **`docker exec -it Ware_Go_postgres psql -U postgres -d ware_go -c "SELECT id, sku, event_type, delta_available, delta_reserved FROM inventory_audit_log;"`**
  * **What it does**: Directly inspects the immutable accounting ledger to verify that stock reservation events (`RESERVE`, `RESTOCK`) were written inside the transaction.

---

### E. `sqlc` (Compile-Time Type-Safe SQL)

* **`sqlc generate`**
  * **What it does**:
    1. Reads `sqlc.yml`.
    2. Parses database schema (`migrations/001_init.sql`).
    3. Validates SQL queries (`internal/repository/queries/*.sql`) against PostgreSQL's actual parser.
    4. Auto-generates type-safe Go structs and query functions in `internal/repository/db/`.
  * **Rule**: Run this whenever you add or modify any `.sql` file in `queries/`.

---

### F. Running the Server & Testing Endpoints

* **`go run cmd/server/main.go`**
  * **What it does**: Starts the HTTP server on port 8080 with Chi router, Postgres connection pool, Redis client, and graceful shutdown.

* **`curl http://localhost:8080/healthz`**
  * **What it does**: Liveness probe checking both PostgreSQL and Redis connectivity.

* **`curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16`**
  * **What it does**: Reads inventory breakdown.
  * **`-i`**: Shows HTTP response headers to observe **`X-Cache: MISS`** (first call from Postgres) vs **`X-Cache: HIT`** (subsequent call from Redis in <0.5ms).

* **`curl -X POST http://localhost:8080/api/v1/orders/reserve -H "Content-Type: application/json" -H "Idempotency-Key: test-key-1" -d "..."`**
  * **What it does**: Reserves stock across warehouses protected by the Idempotency middleware.
  * **Replay Test**: Running the exact same command twice returns the stored order with header **`X-Idempotent-Replay: true`**.

---

### H. Load Testing & Observability (k6 & Grafana Stack) *(Week 6)*

* **`winget install k6 --source winget`**
  * **What it does**: Installs the native Windows binary for Grafana k6 via the Microsoft Package Manager.
  * **Location**: Typically `C:\Program Files\k6\k6.exe`.

* **`k6 version`** (or `"C:\Program Files\k6\k6.exe" version`)
  * **What it does**: Verifies k6 installation and prints version, commit hash, and compiler details.

* **`"C:\Program Files\k6\k6.exe" run test/k6/s1_zero_oversell.js`**
  * **What it does**: Executes Scenario 1 (Zero-Oversell Burst Test) spawning 500 concurrent Virtual Users (VUs) and 2,000 iterations against 100 inventory units.
  * **Threshold Check**: Evaluates `successful_reservations == 100` and verifies remaining stock is `0` in DB.

* **`"C:\Program Files\k6\k6.exe" run test/k6/s2_zombie_stock.js`**
  * **What it does**: Executes Scenario 2 (Zombie Stock & Reaper SLA Test) creating 50 fast-expiring reservations (2s TTL) and benchmarking the Reaper Daemon reclaim latency in milliseconds.
  * **Threshold Check**: Evaluates `reclaim_sla_ms <= 5000` (proving Claim 4: $\le$ 5s recovery).

* **`"C:\Program Files\k6\k6.exe" run test/k6/s3_deadlock.js`**
  * **What it does**: Executes Scenario 3 (Multi-Item Deadlock Freedom) launching 200 concurrent VUs requesting cross-warehouse carts in opposing reverse order.
  * **Threshold Check**: Evaluates `deadlock_errors == 0` (proving Claim 3: Global Lock Hierarchy eliminates deadlocks).

* **`"C:\Program Files\k6\k6.exe" run test/k6/s4_latency.js`**
  * **What it does**: Executes Scenario 4 (Sustained Latency Curve) driving 500 constant arrival RPS for 20 seconds to measure p50, p95, and p99 percentiles under load.
  * **Threshold Check**: Evaluates `http_req_duration: p(95) < 100ms, p(99) < 250ms` (proving Claim 2).

* **`"C:\Program Files\k6\k6.exe" run test/k6/s5_outbox_chaos.js`**
  * **What it does**: Executes Scenario 5 (Transactional Outbox Pipeline) creating and confirming 50 orders in ACID transactions, verifying outbox relay streaming to Kafka `orders.events` and 3PL consumer dispatch.
  * **Threshold Check**: Evaluates 100% confirmation and 0 outbox drops (proving Claim 5).

* **`"C:\Program Files\k6\k6.exe" run test/k6/s6_strategy_benchmark.js`**
  * **What it does**: Executes Scenario 6 (Adaptive Strategy & Contention Benchmark) testing high contention on a single hot SKU vs low contention across 20 distributed cold SKUs.

* **`winget uninstall k6`**
  * **What it does**: Completely uninstalls the k6 binary, removes directories, and cleans the Windows PATH.

* **Observability URLs**:
  * **Prometheus UI**: `http://localhost:9090` (Scrape targets at `/targets`).
  * **Grafana Dashboard**: `http://localhost:3000` (Pre-provisioned dashboard: *Ware_Go — Concurrency & Performance Engine*).

---

### I. Git Workflow

* **`git status`**: Shows modified, staged, and untracked files.
* **`git add .`**: Stages all changed and new files.
* **`git commit -m "feat(...): description"`**: Commits with a conventional commit message.
* **`git push`**: Pushes commits to remote GitHub repository.
* **`del .git\index.lock`**: Deletes a stuck Git lock file if Git crashes.

---

### J. Go Vendoring & Offline Builds *(Week 7)*

* **`go mod vendor`**
  * **What it does**: Copies all third-party dependencies required to build the project into a local `vendor/` directory at the repository root.
  * **Why use it**:
    1. **Eliminates Network Timeouts in Docker**: During `docker build`, `RUN go mod download` often hits `INTERNAL_ERROR` or `unexpected EOF` when fetching packages from `proxy.golang.org`.
    2. **Deterministic & Offline Builds**: With `vendor/`, the Go compiler builds 100% offline without needing internet access or proxy stability.
    3. **Speed**: Decreased Docker build time from 575s (timeout) to **30.1 seconds**.

---

### K. Production Multi-Stage Docker & Compose *(Week 7)*

* **`docker build -t ware_go:latest .`**
  * **What it does**: Compiles the Go binary inside a temporary `golang:alpine` builder using `-mod=vendor -ldflags="-s -w"` and packages it into a minimal `alpine:latest` runner.
  * **Flags**:
    * `-t ware_go:latest`: Tags the resulting container image.
    * `-mod=vendor`: Instructs Go to read modules from the local `vendor/` directory rather than downloading over the internet.
    * `-ldflags="-s -w"`: Strips symbol tables and DWARF debug info, saving ~40% binary size.

* **`docker images ware_go:latest`**
  * **What it does**: Inspects the built container image size and metadata.
  * **Understanding Size Output**:
    * **`CONTENT SIZE` (18.1MB)**: The actual uncompressed size of all filesystem layers in the image. Only these layers are transferred when pushing/pulling from a registry.
    * **`DISK USAGE` (67.7MB)**: The local filesystem space consumed by Docker's overlay2 storage driver, layer cache references, and metadata.

* **`docker compose -f docker-compose.prod.yml up -d`**
  * **What it does**: Starts the full production stack:
    * `app`: `ware_go:latest` running as unprivileged user `appuser` (1 CPU limit, 512MB RAM limit, healthcheck enabled).
    * `postgres`: PostgreSQL 16 on `ware_go_net` network.
    * `redis`: Redis 7 on `ware_go_net` network.
    * `redpanda`: Single-broker Redpanda (Kafka-compatible) on `ware_go_net`.

* **`docker compose -f docker-compose.prod.yml ps`**
  * **What it does**: Checks production container status and healthcheck evaluations (`healthy`).

* **`docker compose -f docker-compose.prod.yml logs -f app`**
  * **What it does**: Follows real-time structured JSON application logs (`slog`) from the production container.

* **`docker compose -f docker-compose.prod.yml down`**
  * **What it does**: Gracefully stops and tears down the production container composition.

* **`docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations\003_rebalance.sql`**
  * **What it does**: Applies Migration 003 (`rebalance_proposals` table and index) to PostgreSQL.

---

### L. AI Inventory Rebalancing Agent *(Week 7)*

* **`curl -X POST http://localhost:8080/api/v1/agent/proposals`**
  * **What it does**: Triggers Google Gemini 3.5 Flash Lite to evaluate current multi-warehouse stock distributions and generate structured rebalancing recommendations.
  * **Database State**: Automatically inserts the proposal into `rebalance_proposals` table with `status = 'PENDING'`.

* **`curl -X POST http://localhost:8080/api/v1/agent/execute-transfer`**
  * **What it does**: Executes the latest `PENDING` proposal inside an ACID transaction:
    * Deducts stock from source warehouse (`TRANSFER_OUT`).
    * Adds stock to target warehouse (`TRANSFER_IN`).
    * Updates proposal `status = 'EXECUTED'`.

* **Duplicate Execution Guard Check**:
  ```bash
  curl -X POST http://localhost:8080/api/v1/agent/execute-transfer
  ```
  * **Expected Output**: Rejects immediately with **`409 Conflict`**:  
    `{"error": "the latest rebalance proposal has already been executed"}`.

* **Inspecting Proposal History in PostgreSQL**:
  ```bash
  docker exec -it Ware_Go_postgres psql -U postgres -d ware_go -c "SELECT id, status, analysis, created_at, executed_at FROM rebalance_proposals ORDER BY id DESC LIMIT 5;"
  ```

---

### M. Go Runtime Performance Profiling (`pprof`) *(Week 6)*

* **`go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30`**
  * **What it does**: Samples CPU instruction pointers at 100Hz across all active goroutines for 30 seconds while running a load test.
  * **Output**: Generates a CPU profile file (e.g. `pprof.warego-server.samples.cpu.001.pb.gz`).

* **`go tool pprof -http=:8081 <profile_file>`**
  * **What it does**: Launches a local web browser interface on port 8081 rendering interactive **Flame Graphs**, Call Trees, and Top CPU consumers.

* **`go tool pprof http://localhost:8080/debug/pprof/heap`**
  * **What it does**: Captures a snapshot of all active memory allocations and in-use heap objects to detect memory leaks.

* **`go tool pprof http://localhost:8080/debug/pprof/goroutine`**
  * **What it does**: Dumps stack traces of every running goroutine to detect leaked or deadlocked goroutines.
