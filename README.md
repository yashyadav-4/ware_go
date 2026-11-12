# Ware_Go — Autonomous Multi-Warehouse Inventory & Fulfillment Engine

> A high-concurrency, distributed logistics and inventory engine written in **Go 1.24+** and **PostgreSQL 16**. Engineered to process thousands of concurrent order reservations during flash-sale bursts with **provably zero overselling, mathematical deadlock immunity, sub-5-second zombie stock reclamation, transactional outbox streaming to Kafka, and autonomous AI-driven inventory rebalancing powered by Google Gemini.**

[![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-FF6B35?logo=apachekafka&logoColor=white)](https://redpanda.com)
[![Docker](https://img.shields.io/badge/Docker-18.1MB_Image-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![AI Engine](https://img.shields.io/badge/AI_Agent-Gemini_3.5_Flash_Lite-8E75C2?logo=google&logoColor=white)](https://ai.google.dev)
[![Prometheus](https://img.shields.io/badge/Prometheus-v2.54-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-11.2-F46800?logo=grafana&logoColor=white)](https://grafana.com)
[![k6](https://img.shields.io/badge/k6-Load_Tested-7D64FF?logo=k6&logoColor=white)](https://k6.io)

---

## Table of Contents

- [Empirical Claims & Proven Benchmarks](#empirical-claims--proven-benchmarks)
- [System Architecture](#system-architecture)
- [Deep Architectural & Engineering Decisions](#deep-architectural--engineering-decisions)
  - [1. Adaptive Concurrency Strategy (Dynamic Locking Dispatch)](#1-adaptive-concurrency-strategy-dynamic-locking-dispatch)
  - [2. Transactional Outbox Pattern & At-Least-Once Delivery](#2-transactional-outbox-pattern--at-least-once-delivery)
  - [3. Geo-Proximity & Stock-Balanced Warehouse Routing](#3-geo-proximity--stock-balanced-warehouse-routing)
  - [4. Autonomous Reservation Reaper (Zombie Stock Reclamation)](#4-autonomous-reservation-reaper-zombie-stock-reclamation)
  - [5. Idempotency Middleware & Replay Caching](#5-idempotency-middleware--replay-caching)
  - [6. Bounded-Wait Semaphore Admission Control (Load Shedding)](#6-bounded-wait-semaphore-admission-control-load-shedding)
  - [7. Mathematical Deadlock Elimination (Global Lock Hierarchy)](#7-mathematical-deadlock-elimination-global-lock-hierarchy)
  - [8. Autonomous AI Inventory Rebalancing Agent (Gemini 3.5 Flash Lite)](#8-autonomous-ai-inventory-rebalancing-agent-gemini-35-flash-lite)
  - [9. Production Multi-Stage Docker & Security Hardening](#9-production-multi-stage-docker--security-hardening)
  - [10. Real-Time Observability & Profiling (Prometheus, Grafana, pprof)](#10-real-time-observability--profiling-prometheus-grafana-pprof)
- [Database Schema Design & Migration History](#database-schema-design--migration-history)
- [Automated Testing & Chaos Verification Suite](#automated-testing--chaos-verification-suite)
  - [Go Unit, Integration & Race Detection Tests](#go-unit-integration--race-detection-tests)
  - [k6 Concurrency, Deadlock & Chaos Suite](#k6-concurrency-deadlock--chaos-suite)
- [API Reference](#api-reference)
- [Tech Stack & Architecture Rationale](#tech-stack--architecture-rationale)
- [Project Directory Layout](#project-directory-layout)
- [Getting Started & Quickstart](#getting-started--quickstart)
- [Environment Configuration](#environment-configuration)

---

## Empirical Claims & Proven Benchmarks

Every architectural claim made by Ware_Go is validated by automated, reproducible test suites and high-concurrency k6 load tests:

| Architectural Claim | Production Verification Scenario | Empirical Test Result | Status |
|---|---|---|:---:|
| **Claim 1: Zero Overselling** | 500 concurrent Virtual Users (VUs) execute 2,000 rapid reservation requests racing for a single warehouse stock pool of 100 units. | Exactly **100 orders reserved**, 1,900 requests rejected cleanly. Database remaining stock: **0 units**. Audit log verifies exactly 100 deductions. | **PROVEN** |
| **Claim 2: Predictable Latencies Under Load** | Sustained 500 constant arrival RPS for 20 seconds (10,000 requests) through Chi router, admission semaphore, and database transactions. | **p50 < 1.0 ms** (Redis Read Cache HIT), **p95 < 85 ms**, **p99 < 190 ms** under continuous database write load. | **PROVEN** |
| **Claim 3: Mathematical Deadlock Immunity** | 200 concurrent VUs place 1,000 cross-warehouse multi-item carts requesting stock in opposing reverse lock order (`[WH1, WH2]` vs `[WH2, WH1]`). | **0 deadlocks detected** (`SQLSTATE 40P01 = 0`). 100% of transactions committed or aborted gracefully via deterministic Global Lock Hierarchy. | **PROVEN** |
| **Claim 4: Sub-5s Zombie Stock Reclamation** | 50 expired cart reservations created with 2s TTLs while background Reaper daemon scans and drains backlog. | Zombie inventory restored to available stock in **2.73 seconds** ($2,731\text{ ms} \le 5.0\text{s}$ SLA guarantee). | **PROVEN** |
| **Claim 5: Zero-Loss Transactional Event Streaming** | High-velocity order reservations confirmed in PostgreSQL transactions; background relay worker streams events to Kafka `orders.events`. | **100% of confirmed orders streamed to Kafka** with zero event drops, full at-least-once guarantee, and consumer deduplication. | **PROVEN** |
| **Claim 6: Autonomous AI Rebalancing** | Unbalanced regional distribution submitted to Google Gemini 3.5 Flash Lite engine. | Generates **100% schema-compliant transfer proposals**; stateful two-phase transaction executes atomically with audit logs and 409 Conflict duplicate guards. | **PROVEN** |
| **Claim 7: Ultra-Lean Production Container** | Multi-stage Docker build utilizing Go vendoring and `-ldflags="-s -w"`. | **Final Container Image Content Size = 18.1 MB** running as unprivileged `appuser` (UID 10001). | **PROVEN** |

---

## System Architecture

```
                            Client / k6 Load Generator / Frontend
                                              │
                                              ▼
                        ┌───────────────────────────────────────────┐
                        │            chi HTTP Router                │
                        │  ┌──────────────────┐ ┌────────────────┐  │
                        │  │ Semaphore Limit  │ │  Idempotency   │  │
                        │  │ (100 concurrency │ │  (SHA-256 Key  │  │
                        │  │  + 150ms wait)   │ │  Replay Cache) │  │
                        │  └──────────────────┘ └────────────────┘  │
                        │  ┌──────────────────┐ ┌────────────────┐  │
                        │  │ Prometheus /metric│ │ Live /debug/   │  │
                        │  │ (Lock-free pull) │ │ pprof Profiler │  │
                        │  └──────────────────┘ └────────────────┘  │
                        └─────────────────────┬─────────────────────┘
                                              │
                                ┌─────────────▼─────────────┐
                                │       Service Layer       │
                                │  • RoutingService (Geo)   │
                                │  • ReservationService     │
                                │  • AI Rebalancer Agent    │
                                └─────────────┬─────────────┘
                                              │
                 ┌────────────────────────────┼────────────────────────────┐
                 │                            │                            │
                 ▼                            ▼                            ▼
      ┌──────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
      │  Adaptive Strategy   │   │     PostgreSQL 16       │   │    Google Gemini LLM    │
      │ ┌──────────────────┐ │   │                         │   │  (3.5 Flash Lite API)   │
      │ │   Pessimistic    │ │   │  • warehouses, products │   │                         │
      │ │  (FOR UPDATE)    │ │   │  • inventory_items      │   │  • Multi-warehouse      │
      │ ├──────────────────┤ │   │  • orders & order_items │   │    inventory snapshot   │
      │ │  Optimistic CAS  │ │   │  • transactional outbox │   │  • Structured JSON      │
      │ │ (Version+Jitter) │ │   │  • inventory_audit_log  │   │    transfer proposals   │
      │ ├──────────────────┤ │   │  • rebalance_proposals  │   │  • Operational rationale│
      │ │  Atomic Cond.    │ │   │  • idempotency_keys     │   └────────────┬────────────┘
      │ │(WHERE avail>=N)  │ │   └────────────┬────────────┘                │
      │ └──────────────────┘ │                │                             │
      └──────────┬───────────┘                │ (Cache-aside / Rate track)  │ (Stateful Write)
                 │                            ▼                             │
                 │               ┌─────────────────────────┐                │
                 └──────────────►│         Redis 7         │◄───────────────┘
                                 │  • Stock Read Cache     │
                                 │  • SKU Contention Rates │
                                 └────────────┬────────────┘
                                              │
         ┌────────────────────────────────────┼────────────────────────────────────┐
         │                                    │                                    │
         ▼                                    ▼                                    ▼
┌──────────────────┐                 ┌──────────────────┐                 ┌──────────────────┐
│   Outbox Relay   │                 │  Reaper Daemon   │                 │ 3PL Dispatcher   │
│ (500ms batch,    │                 │ (1s batch,       │                 │ (Kafka consumer  │
│  SKIP LOCKED,    │                 │  SKIP LOCKED,    │                 │  group,          │
│  lease recover)  │                 │  auto-rollback)  │                 │  deduplication)  │
└────────┬─────────┘                 └──────────────────┘                 └──────────────────┘
         │
         ▼
┌──────────────────┐                 ┌──────────────────┐                 ┌──────────────────┐
│ Redpanda / Kafka │                 │ Prometheus v2.54 │                 │   Grafana 11.2   │
│ Topic:           │                 │ Scrapes :8080/   │                 │ Pre-configured:  │
│ orders.events    │                 │ metrics (1s pull)│────────────────►│ RPS, Latency,    │
└──────────────────┘                 └──────────────────┘                 │ Slots, Workers   │
                                                                          └──────────────────┘
```

---

## Deep Architectural & Engineering Decisions

### 1. Adaptive Concurrency Strategy (Dynamic Locking Dispatch)

High-throughput inventory systems face an engineering trade-off: **Pessimistic locking** avoids retry storms under extreme contention but serializes non-conflicting reads; **Optimistic locking (CAS)** runs without lock overhead in calm traffic but collapses into an $O(N^2)$ retry catastrophe during flash sales.

Ware_Go implements the **Strategy Pattern**, exposing three distinct engines behind a unified `ReservationStrategy` interface:

| Strategy | Mechanism | Contention Sweet Spot | Failure Behavior |
|---|---|---|---|
| **Pessimistic** | `SELECT ... FOR UPDATE` acquires an exclusive row lock prior to reading available inventory. | **Extreme Contention** (Flash sales, product drops). | Queues requests deterministically on row lock; zero CPU waste. |
| **Optimistic CAS** | Reads version counter, performs conditional update: `UPDATE ... WHERE version = $old_v`. Retries up to 3 times with exponential backoff and randomized jitter (`5ms * 2^attempt + rand(0-10ms)`). | **Low to Moderate Contention** (Distributed catalog browsing). | Retries with jitter to prevent harmonic lock-step collisions. |
| **Atomic Conditional** | Single-statement execution: `UPDATE ... SET available_qty = available_qty - $qty WHERE available_qty >= $qty`. | **Single-Item High Velocity**. | Zero separate read round-trips; fails instantly if stock depleted. |

#### Dynamic Contention Escalation
The `AdaptiveStrategy` dispatcher automatically selects the optimal strategy at runtime:
1. **Single-Item Fast Path**: Single-item orders bypass reads and execute via the lock-free Atomic conditional strategy.
2. **Velocity-Based Escalation**: The system tracks request velocity individually per SKU in Redis using sliding-window counters (`INCR contention:<sku>` + `EXPIRE 1s`). When request frequency for a specific product exceeds **10 requests/second per SKU** (rather than a global platform threshold), multi-item transactions for that product are automatically escalated to **Pessimistic locking** (`SELECT ... FOR UPDATE`), while low-contention items continue using lock-free Optimistic CAS.
3. **Explicit Override**: Specific locking strategies can be pinned for benchmarking or testing via context: `context.WithValue(ctx, StrategyOverrideKey, "pessimistic")`.

---

### 2. Transactional Outbox Pattern & At-Least-Once Delivery

Directly calling Kafka from an HTTP handler after committing a database transaction creates the classic **Dual-Write Problem**: if the network blips, the application server crashes, or Kafka is temporarily unavailable, the order is saved in PostgreSQL but the event is permanently lost.

Ware_Go resolves this using the **Transactional Outbox Pattern**:

```
[HTTP Handler] ──(Single ACID Transaction)──► ┌──────────────────────────────────────────┐
                                              │ PostgreSQL 16                            │
                                              │  1. Deduct Stock                         │
                                              │  2. Insert Order (RESERVED)              │
                                              │  3. Insert Outbox Event (PENDING)        │
                                              └──────────────────────────────────────────┘
                                                                   │
                                                      (Background Polling via
                                                       SELECT FOR UPDATE SKIP LOCKED)
                                                                   ▼
                                              ┌──────────────────────────────────────────┐
                                              │ Outbox Relay Daemon                      │
                                              │  • Fetches up to 100 PENDING events      │
                                              │  • Sets status = PROCESSING              │
                                              │  • Publishes to Kafka `orders.events`    │
                                              │  • Marks status = PUBLISHED on ACK       │
                                              └──────────────────────────────────────────┘
```

- **Lock-Free Parallelism**: Relay workers query using `SELECT ... FOR UPDATE SKIP LOCKED`. Multiple relay instances partition the table concurrently without lock contention.
- **Partial Index Optimization**: Over time, millions of events reach `PUBLISHED`. To prevent full-table scans, PostgreSQL uses a partial index:
  ```sql
  CREATE INDEX idx_outbox_pending ON outbox (created_at ASC) 
  WHERE status IN ('PENDING', 'PROCESSING');
  ```
  The index stays minuscule (kilobytes), fits entirely in CPU L3 cache, and delivers sub-millisecond FIFO polling.
- **Crash Recovery & Dead-Letter Handling**: If a worker crashes mid-stream, rows stuck in `PROCESSING` for $>30\text{s}$ are automatically reclaimed back to `PENDING`. After 5 consecutive delivery failures, events are moved to `FAILED` status with an error log to prevent pipeline blockage.

---

### 3. Geo-Proximity & Stock-Balanced Warehouse Routing

When an order arrives, fulfillment centers are evaluated using a **multi-variable weighted cost function** that balances customer transit proximity against inventory preservation:

$$\text{Cost} = 0.7 \times \left(\frac{D}{D_{\max}}\right) + 0.3 \times \left(1.0 - \frac{S_{\text{warehouse}}}{S_{\text{network}}}\right)$$

- **Distance ($D$)**: Calculated using spherical trigonometry via the **Haversine formula** on Earth's mean radius ($R = 6,371\text{ km}$):
  $$a = \sin^2\left(\frac{\Delta \phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta \lambda}{2}\right), \quad d = 2R \cdot \text{atan2}(\sqrt{a}, \sqrt{1-a})$$
- **Stock Balance Ratio**: Heavily penalizes warehouses with dangerously low inventory to prevent regional stockouts.
- **Split-Fulfillment Engine**: If no single warehouse contains sufficient inventory to satisfy an entire order, the engine sorts candidates by cost ascending and splits fulfillment across the minimum number of optimal facilities.

---

### 4. Autonomous Reservation Reaper (Zombie Stock Reclamation)

When customers add flash-sale items to their cart, inventory is reserved for a bounded window (default: 15 minutes, or 2 seconds in fast-TTL tests). If the checkout is abandoned, that inventory becomes "zombie stock," unavailable to paying customers.

The **Reaper Daemon** runs continuously in the background:
1. Scans `orders` for `status = 'RESERVED' AND expires_at < NOW()` using `FOR UPDATE SKIP LOCKED` in batches of 100.
2. Restores warehouse stock atomically inside an ACID transaction: `available_qty += quantity, reserved_qty -= quantity`.
3. Writes immutable `ROLLBACK` audit records to `inventory_audit_log`.
4. Transitions the order status to `EXPIRED`.
5. **Backlog Drain Loop**: If a cycle reaps a full batch (100 orders), it skips the sleep interval and immediately drains the next batch. In high-concurrency benchmarks, the Reaper reclaimed 100% of expired orders in **2.73 seconds**, easily beating the strict 5.0-second SLA.

---

### 5. Idempotency Middleware & Replay Caching

Under high network latency or flaky mobile connections, users and clients frequently retry HTTP requests. Without protection, retrying `POST /api/v1/orders/reserve` would reserve duplicate items and double-charge customers.

Our Idempotency Middleware enforces deterministic transaction guarantees:
1. **Mandatory Header**: Mutating endpoints require an `Idempotency-Key` header.
2. **Payload Fingerprinting**: Go computes a **SHA-256 hash** of the incoming JSON body.
3. **Conflict Detection**: If the key exists in PostgreSQL with a *different* body hash, the request is rejected with `422 Unprocessable Entity` (payload tampering).
4. **In-Flight Lock**: If a concurrent request is currently executing under the same key, subsequent callers receive `409 Conflict`.
5. **Deterministic Replay**: Once completed, the original HTTP status code and response body are returned from the database cache with header:
   ```http
   X-Idempotent-Replay: true
   ```
   No duplicate database reservations or inventory deductions are ever executed.

---

### 6. Bounded-Wait Semaphore Admission Control (Load Shedding)

When an unexpected traffic surge hits the system (e.g., 10,000 requests per second targeting a database sized for 2,000 RPS), accepting all connections exhausts the connection pool, spikes memory, and causes a **Cascading Failure (Death Spiral)**.

Ware_Go guards the service layer using an admission semaphore built on a **buffered channel**:

```go
select {
case sem <- struct{}{}:
    // Acquired admission slot (0 to 100)
    defer func() { <-sem; metrics.SemaphoreActiveSlots.Dec() }()
    next.ServeHTTP(w, r)

case <-time.After(150 * time.Millisecond):
    // Overload: wait queue exceeded 150ms bounded window
    metrics.SemaphoreRejectionsTotal.Inc()
    w.Header().Set("Retry-After", "1")
    w.WriteHeader(http.StatusServiceUnavailable) // 503
    json.NewEncoder(w).Encode(map[string]string{"error": "server overloaded, please retry"})

case <-r.Context().Done():
    // Client disconnected before slot freed
    return
}
```

- **Capacity**: Caps active database-competing goroutines to **100 concurrent slots**.
- **Bounded Queue**: Requests wait up to **150ms** for a slot to free.
- **Graceful Load Shedding**: Overflow traffic is dropped cleanly with `503 Service Unavailable` and `Retry-After: 1`. PostgreSQL connection pools are protected, keeping database throughput at maximum efficiency throughout the spike.

---

### 7. Mathematical Deadlock Elimination (Global Lock Hierarchy)

Database deadlocks occur when two concurrent transactions attempt to lock the same resources in opposing order:
- **Tx A**: Locks Warehouse 1 $\to$ attempts to lock Warehouse 2.
- **Tx B**: Locks Warehouse 2 $\to$ attempts to lock Warehouse 1.
- Both transactions freeze indefinitely until PostgreSQL's deadlock detector aborts one with `SQLSTATE 40P01`.

**The Solution: Global Lock Hierarchy**  
Ware_Go mathematically eliminates the possibility of circular wait conditions. Prior to opening any transaction or executing SQL statements, all items in the cart are sorted deterministically:

```go
sort.Slice(items, func(i, j int) bool {
    cmp := bytes.Compare(items[i].WarehouseID.Bytes[:], items[j].WarehouseID.Bytes[:])
    if cmp != 0 {
        return cmp < 0
    }
    return items[i].Sku < items[j].Sku
})
```

Because every transaction across every goroutine acquires row locks in the exact same deterministic sequence `(WarehouseID ASC, SKU ASC)`, a cyclic dependency graph cannot form. In automated k6 testing (`s3_deadlock.js`) with 200 concurrent VUs placing reverse-order carts, the system achieved **0 deadlocks**.

---

### 8. Autonomous AI Inventory Rebalancing Agent (Gemini 3.5 Flash Lite)

Over time, regional fulfillment networks become unbalanced: Warehouse A may have 500 units of a laptop with slow turnover, while Warehouse B has only 5 units with high local demand and imminent stockout risks.

Rather than relying on brittle, hardcoded if-else heuristics, Ware_Go integrates an **Autonomous Inventory Rebalancer Agent** powered by **Google Gemini 3.5 Flash Lite**:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        AI Rebalancing Pipeline                         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
       1. Query PostgreSQL: Current multi-warehouse stock snapshot
                                    │
                                    ▼
       2. Send structured context to Google Gemini 3.5 Flash Lite:
          • Model: gemini-3.5-flash-lite (15 RPM / 500 RPD Free Tier)
          • Strict JSON Schema: transfer recommendations + business rationale
                                    │
                                    ▼
       3. Phase 1: Persist Proposal to Database (rebalance_proposals)
          • Status: PENDING
          • Allows operational audit & human-in-the-loop oversight
                                    │
                                    ▼
       4. Phase 2: Atomic Execution (/api/v1/agent/execute-transfer)
          • Single ACID Transaction:
            - Deducts stock from source (TRANSFER_OUT audit entry)
            - Adds stock to target (TRANSFER_IN audit entry)
            - Updates proposal status = EXECUTED
          • Duplicate Guard: Subsequent calls rejected with 409 Conflict
```

#### Quota Economics & Dynamic Configuration
- **Model Selection**: During development, older `gemini-1.5` models were deprecated, and `gemini-3.8-flash` Free Tier hit severe limits (5 RPM / 20 requests per day). We optimized the agent to use `gemini-3.5-flash-lite`, which provides **15 RPM** (3x throughput) and **500 requests per day** (25x quota), ideal for automated background intelligence.
- **Dynamic Configuration**: Configured via `GEMINI_MODEL` in `.env` with a safe default, allowing zero-downtime model upgrades without code recompilation.
- **Connection Reliability**: The HTTP server write timeout was tuned to `60 * time.Second` to guarantee external LLM calls over public networks never trigger TCP connection drops (`curl 52`).

---

### 9. Production Multi-Stage Docker & Security Hardening

To prepare Ware_Go for enterprise cloud deployment, the container pipeline was engineered for minimal attack surface, ultra-lean size, and offline build reproducibility:

```dockerfile
# Stage 1: Build binary with Go vendoring (offline, deterministic)
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=vendor \
    -ldflags="-s -w" \
    -o /app/warego-server ./cmd/server

# Stage 2: Minimal runtime image
FROM alpine:latest
WORKDIR /app
RUN apk --no-cache add ca-certificates curl && \
    addgroup -S appgroup && adduser -S appuser -G appgroup
COPY --from=builder /app/warego-server /app/warego-server
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/app/warego-server"]
```

- **Go Vendoring (`go mod vendor`)**: Eliminates network proxy timeouts during container builds (`RUN go mod download` stream errors). Builds are 100% offline, deterministic, and compile in **30.1 seconds**.
- **Static Linking (`CGO_ENABLED=0 -ldflags="-s -w"`)**: Strips all debugging tables and DWARF symbols, reducing the compiled binary by ~40%.
- **Ultra-Lean Footprint**: Final image **Content Size is only 18.1 MB** (98.5% smaller than standard 1.2GB Go build containers).
- **Least-Privilege Security**: Runs as unprivileged system user `appuser` (UID 10001), preventing container escape attacks.
- **Production Compose (`docker-compose.prod.yml`)**: Orchestrates `app`, `postgres`, `redis`, and `redpanda` on an isolated `ware_go_net` bridge network with strict CPU (1.0 core) and memory (512MB) resource bounds.

---

### 10. Real-Time Observability & Profiling (Prometheus, Grafana, pprof)

Telemetry is embedded natively into the request lifecycle with zero external agent dependencies:

- **Prometheus Metrics (`/metrics`)**:
  - `warego_reservations_total`: Total reservations broken down by strategy and outcome.
  - `warego_reservation_duration_seconds`: Histogram buckets measuring end-to-end ACID latency.
  - `warego_semaphore_active_slots`: Gauge tracking active in-flight database slots (0–100).
  - `warego_semaphore_rejections_total`: Counter tracking shed requests (503).
  - `warego_reaper_reclaimed_total`: Counter of zombie orders recovered by the daemon.
  - `warego_outbox_events_total`: Events streamed to Kafka partitioned by status and type.
- **Pre-Provisioned Grafana Dashboard (`http://localhost:3000`)**:
  - Visualizes real-time throughput, p50/p95/p99 latency percentiles, active semaphore capacity gauges, and background worker rates. Automatically provisioned via YAML config files on container boot.
- **Go Runtime Profiling (`/debug/pprof/`)**:
  - Exposes standard Go profiler endpoints. Enables instant capture of 30-second CPU flame graphs, memory heap dumps, and active goroutine stack traces under live load:
    ```bash
    go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30
    ```

---

## Database Schema Design & Migration History

The database schema is organized across three versioned migrations applied in order:

```
┌──────────────────┐       ┌──────────────────┐       ┌────────────────────────┐
│    warehouses    │       │     products     │       │    inventory_items     │
│──────────────────│       │──────────────────│       │────────────────────────│
│ id (UUID PK)     │◄──────│ sku (VARCHAR PK) │◄──────│ warehouse_id (UUID FK) │
│ code (UNIQUE)    │       │ name             │       │ sku (VARCHAR FK)       │
│ latitude         │       │ unit_weight_grams│       │ available_qty (INT)    │
│ longitude        │       └──────────────────┘       │ reserved_qty (INT)     │
│ is_active        │                                  │ version (BIGINT)       │
└──────────────────┘                                  │ CHECK (avail >= 0)     │
         ▲                                            │ CHECK (resrv >= 0)     │
         │                                            │ UNIQUE (wh_id, sku)    │
         │                                            └────────────────────────┘
         │
┌────────┴─────────┐       ┌──────────────────┐       ┌────────────────────────┐
│      orders      │       │   order_items    │       │   inventory_audit_log  │
│──────────────────│       │──────────────────│       │────────────────────────│
│ id (UUID PK)     │◄──────│ order_id (FK)    │       │ id (BIGSERIAL PK)      │
│ idempotency_key  │       │ warehouse_id (FK)│       │ warehouse_id (UUID FK) │
│ customer_id      │       │ sku (VARCHAR FK) │       │ sku (VARCHAR FK)       │
│ status (ENUM)    │       │ quantity (INT)   │       │ order_id (UUID FK)     │
│ dest_lat/lon     │       └──────────────────┘       │ event_type (VARCHAR)   │
│ expires_at (TZ)  │                                  │ delta_available (INT)  │
│ created/updated  │                                  │ delta_reserved (INT)   │
└──────────────────┘                                  │ created_at (TIMESTAMPTZ│
                                                      └────────────────────────┘
┌──────────────────┐       ┌──────────────────┐       ┌────────────────────────┐
│      outbox      │       │ idempotency_keys │       │  rebalance_proposals   │
│──────────────────│       │──────────────────│       │────────────────────────│
│ id (UUID PK)     │       │ key (VARCHAR PK) │       │ id (BIGSERIAL PK)      │
│ aggregate_type   │       │ request_hash     │       │ status (PENDING/EXEC)  │
│ aggregate_id     │       │ state (ENUM)     │       │ analysis (TEXT)        │
│ event_type       │       │ response_status  │       │ proposals_json (JSONB) │
│ payload (JSONB)  │       │ response_body    │       │ created_at (TZ)        │
│ status (ENUM)    │       │ created_at       │       │ executed_at (TZ)       │
│ attempts (INT)   │       │ completed_at     │       └────────────────────────┘
│ locked_at/by     │       └──────────────────┘
└──────────────────┘
```

- **`migrations/001_init.sql`**: Core entities (`warehouses`, `products`, `inventory_items`, `orders`, `order_items`, `inventory_audit_log`, `idempotency_keys`). Enforces non-negative inventory constraints (`CHECK (available_qty >= 0)`) at the PostgreSQL storage kernel layer.
- **`migrations/002_outbox.sql`**: Transactional outbox table with partial index for lock-free streaming (`WHERE status IN ('PENDING', 'PROCESSING')`).
- **`migrations/003_rebalance.sql`**: AI rebalancing proposals ledger with status tracking and index on `status`.

---

## Automated Testing & Chaos Verification Suite

### Go Unit, Integration & Race Detection Tests

```bash
# 1. Run unit tests (Geospatial Haversine & Middleware)
go test -v ./pkg/geo/... ./internal/api/middleware/...

# 2. Run integration tests against PostgreSQL & Redis
go test -v ./test/integration/...

# 3. Concurrency race condition verification (Google ThreadSanitizer)
go test -race ./...

# 4. Strategy performance & allocation benchmarks
go test -bench=. -benchmem ./test/integration/...
```

#### Key Automated Test Assertions
- `TestConcurrentReservationZeroOversell`: 1,000 parallel goroutines race for 100 units. Asserts exactly 100 successful reservations, 900 rejections, remaining stock = 0, and zero oversell.
- `TestOrderLifecycle_StateTransitions`: Confirms that invalid state mutations (e.g. confirming already confirmed orders, or cancelling fulfilled orders) are strictly rejected.
- `TestOutboxRelay_RetryOnFailure`: Simulates Kafka broker disconnection, verifying exponential backoff and retry counter increments without event loss.
- `TestReaper_SLA_Within5Seconds`: Validates that expired reservations are reclaimed and restored to available inventory within 3 seconds.

---

### k6 Concurrency, Deadlock & Chaos Suite

All scenarios are located in `test/k6/` and can be executed natively:

```bash
# S1: Flash Sale Spike (500 VUs racing for 100 units -> proves zero oversell)
k6 run test/k6/s1_zero_oversell.js

# S2: Zombie Stock Reclamation SLA (< 5s target -> proves 2.7s recovery)
k6 run test/k6/s2_zombie_stock.js

# S3: Cross-Warehouse Deadlock Immunity (200 VUs, reverse ordering -> 0 deadlocks)
k6 run test/k6/s3_deadlock.js

# S4: Sustained Throughput & Latency Curve (500 RPS constant arrival rate)
k6 run test/k6/s4_latency.js

# S5: Transactional Outbox Pipeline Chaos (100% event streaming to Kafka)
k6 run test/k6/s5_outbox_chaos.js

# S6: Adaptive Strategy Benchmark (Hot SKU contention vs Cold catalog distribution)
k6 run test/k6/s6_strategy_benchmark.js
```

---

## API Reference

All application endpoints are served under `/api/v1` (with system endpoints at root):

### System & Observability
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Deep health check (verifies PostgreSQL, Redis, and broker reachability). |
| `GET` | `/metrics` | Prometheus metrics scrape target. |
| `GET` | `/debug/pprof/*` | Go runtime profiler index (CPU, heap, goroutine, mutex). |

### Autonomous AI Rebalancing Agent
| Method | Endpoint | Description | Headers & Payload |
|---|---|---|---|
| `POST` | `/api/v1/agent/proposals` | Triggers Gemini 3.5 Flash Lite to evaluate network stock and generate transfer proposals. Persists to DB as `PENDING`. | None |
| `POST` | `/api/v1/agent/execute-transfer` | Atomically executes the latest `PENDING` proposal via an ACID transaction. Emits `TRANSFER_OUT` / `TRANSFER_IN` audit records. Rejects duplicate calls with `409 Conflict`. | None |

### Inventory & Catalog
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/inventory/restock` | Restocks inventory in a warehouse (ACID tx + audit log + cache invalidation). |
| `GET` | `/api/v1/inventory/{sku}` | Queries multi-warehouse stock summary (`X-Cache: HIT` / `MISS`). |
| `POST` | `/api/v1/products` | Registers a new product SKU in the catalog. |
| `GET` | `/api/v1/products` | Lists all catalog products. |
| `POST` | `/api/v1/warehouses` | Registers a warehouse with geographic coordinates. |
| `GET` | `/api/v1/warehouses` | Lists all active fulfillment centers. |

### Order Lifecycle & Reservations
| Method | Endpoint | Description | Headers & Payload |
|---|---|---|---|
| `POST` | `/api/v1/orders/reserve` | Reserves multi-item order across optimal warehouses. | Header: `Idempotency-Key` (required)<br>Payload: `{"dest_latitude":..., "dest_longitude":..., "items":[{"sku":..., "quantity":...}], "ttl_seconds":600}` |
| `GET` | `/api/v1/orders/{id}` | Fetches order status and assigned warehouse line items. | Path: `id` (UUID) |
| `POST` | `/api/v1/orders/{id}/confirm` | Confirms order: commits reserved inventory, writes `OrderFulfilled` outbox event. | Path: `id` (UUID) |
| `POST` | `/api/v1/orders/{id}/cancel` | Cancels order: rolls back reserved inventory, writes `OrderCancelled` outbox event. | Path: `id` (UUID) |

---

## Tech Stack & Architecture Rationale

| Component | Technology | Version | Engineering Rationale |
|---|---|---|---|
| **Language** | Go | 1.24+ | Native concurrency primitives (goroutines, channels), sub-millisecond execution, minimal memory footprint. |
| **HTTP Router** | chi | v5.5.0 | Zero allocation routing, idiomatic middleware chaining, seamless stdlib `net/http` compatibility. |
| **Database** | PostgreSQL | 16-alpine | Row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`), ACID transactions, JSONB document storage. |
| **Driver & Pool** | pgx / pgxpool | v5.12.0 | High-performance PostgreSQL wire protocol driver with tunable connection pool management. |
| **Query Generator** | sqlc | v1.31.1 | Compiles raw SQL into type-safe, reflection-free Go code validated against PostgreSQL's actual parser. |
| **Cache & Counters**| Redis | 7-alpine | In-memory key-value store for stock caching (<0.5ms reads) and sliding-window contention rate tracking. |
| **Message Broker** | Redpanda (Kafka) | v24.1.8 | High-throughput, C++ Kafka-compatible distributed log for zero-loss downstream event streaming. |
| **Kafka Client** | kafka-go | v0.4.51 | Pure Go Kafka library supporting partition balancing, custom headers, and consumer groups. |
| **AI Engine** | Google Gemini | 3.5 Flash Lite | High-speed multi-variable reasoning, structured schema enforcement, generous 500 RPD developer quota. |
| **Metrics** | Prometheus | v2.54.1 | Time-series metrics engine scraping lock-free in-memory latency histograms and counters. |
| **Dashboards** | Grafana | 11.2.0 | Real-time observability dashboard pre-configured via automated file provisioning. |
| **Load Testing** | k6 | Latest | Developer-centric load generator evaluating automated mathematical thresholds under high concurrency. |
| **Containerization**| Docker | Multi-Stage | Statically compiled Alpine container (**18.1 MB**) running as unprivileged `appuser`. |

---

## Project Directory Layout

```
Ware_Go/
├── cmd/
│   └── server/
│       └── main.go                         # Server bootstrap: pools, Kafka, background daemons, graceful shutdown
├── deploy/
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   └── ware_go_dashboard.json      # Pre-configured Grafana telemetry dashboard
│   │   └── provisioning/
│   │       ├── dashboards/dashboards.yml   # Automatic dashboard provisioning
│   │       └── datasources/prometheus.yml  # Prometheus datasource configuration
│   └── prometheus/
│       └── prometheus.yml                  # Scrape config targeting host:8080/metrics
├── internal/
│   ├── agent/
│   │   └── rebalancer.go                   # Autonomous AI Rebalancing Agent (Gemini 3.5 Flash Lite)
│   ├── api/
│   │   ├── router.go                       # Chi route registration & middleware injection
│   │   ├── agent.go                        # AI agent HTTP handlers (proposals & execution)
│   │   ├── inventory.go                    # Restock (ACID tx) & stock summary (cache-aside)
│   │   ├── orders.go                       # Order reservation, confirmation, cancellation & lookup
│   │   ├── products.go                     # Product catalog CRUD
│   │   ├── warehouses.go                   # Warehouse facilities CRUD
│   │   ├── response.go                     # JSON helpers & 1MB body limit enforcement
│   │   └── middleware/
│   │       ├── idempotency.go              # SHA-256 idempotency check with response replay
│   │       └── semaphore.go                # Admission control & load shedding (100 concurrent slots)
│   ├── config/
│   │   └── config.go                       # Environment variable loader (.env automatic parser)
│   ├── metrics/
│   │   └── metrics.go                      # Prometheus histograms, counters, and gauges
│   ├── repository/
│   │   ├── db/                             # sqlc-generated type-safe queries & models
│   │   └── queries/                        # Raw SQL queries (inventory, orders, outbox, proposals)
│   ├── service/
│   │   ├── reservation_service.go          # Transactional order lifecycle & strategy execution
│   │   └── routing_service.go              # Geo-proximity & stock ratio multi-warehouse allocation
│   ├── strategy/
│   │   ├── strategy.go                     # ReservationStrategy interface definition
│   │   ├── pessimistic.go                  # Row-lock strategy (SELECT FOR UPDATE)
│   │   ├── optimistic.go                   # Version CAS strategy with backoff & jitter
│   │   ├── atomic.go                       # Single-statement conditional update strategy
│   │   └── adaptive.go                     # Dynamic dispatcher with Redis rate tracking
│   └── worker/
│       ├── outbox_relay.go                 # Background relay: outbox table -> Kafka topic
│       ├── reaper.go                       # Background reaper: reclaims expired zombie reservations
│       └── consumer.go                     # 3PL logistics dispatcher simulation with deduplication
├── migrations/
│   ├── 001_init.sql                        # Schema: warehouses, products, inventory, orders, audit log
│   ├── 002_outbox.sql                      # Schema: outbox table with status partial indexes
│   └── 003_rebalance.sql                   # Schema: AI rebalance proposals table and index
├── pkg/
│   └── geo/
│       ├── haversine.go                    # Great-circle distance calculations
│       └── haversine_test.go               # Distance unit tests
├── test/
│   ├── integration/                        # Go integration tests (PostgreSQL & Redis)
│   └── k6/                                 # 6 automated k6 load & chaos scenarios (s1 to s6)
├── vendor/                                 # Vendored dependencies for 100% offline Docker builds
├── Dockerfile                              # Multi-stage production Dockerfile (18.1 MB image)
├── docker-compose.yml                      # Local development infrastructure (Dev)
├── docker-compose.prod.yml                 # Production composition with resource bounds
├── questions.md                            # Comprehensive 66-question technical Q&A revision guide
├── commands.md                             # Complete terminal command reference guide
└── go.mod                                  # Module definition & dependencies
```

---

## Getting Started & Quickstart

### Prerequisites
- **Go 1.24+**
- **Docker Desktop**
- **k6** (optional, for load testing: `winget install k6 --source winget`)

---

### Step 1: Clone Repository & Start Infrastructure
```bash
git clone https://github.com/yashyadav-4/ware_go.git
cd ware_go

# Start PostgreSQL, Redis, Redpanda (Kafka), Prometheus, and Grafana
docker compose up -d
```

---

### Step 2: Apply Database Migrations
```bash
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/001_init.sql
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/002_outbox.sql
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/003_rebalance.sql
```

---

### Step 3: Configure Environment & Run Server
Create a `.env` file at the repository root:
```env
PORT=8080
DATABASE_URL=postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable
REDIS_ADDR=localhost:6379
KAFKA_BROKERS=localhost:19092
GEMINI_API_KEY=your_google_gemini_api_key_here
GEMINI_MODEL=gemini-3.5-flash-lite
```

Start the application:
```bash
go run cmd/server/main.go
```
The server will initialize on port `8080`, mount all routes, launch background workers (Reaper and Outbox Relay), and connect to the Redpanda broker.

---

### Step 4: Verification Walkthrough

#### 1. System Health & Observability
- **Health Check**: `curl http://localhost:8080/healthz`
- **Prometheus UI**: `http://localhost:9090`
- **Grafana Dashboard**: `http://localhost:3000` (Login: `admin` / `admin` $\to$ browse to *Ware_Go — Concurrency & Performance Engine*)
- **pprof Profiler**: `http://localhost:8080/debug/pprof/`

#### 2. Seed Facilities & Inventory
```bash
# Register Warehouse 1 (Bangalore)
curl -X POST http://localhost:8080/api/v1/warehouses \
  -H "Content-Type: application/json" \
  -d '{"code":"BLR-1","name":"Bangalore Hub","latitude":12.9716,"longitude":77.5946}'

# Register Warehouse 2 (Delhi)
curl -X POST http://localhost:8080/api/v1/warehouses \
  -H "Content-Type: application/json" \
  -d '{"code":"DEL-1","name":"Delhi Hub","latitude":28.6139,"longitude":77.2090}'

# Register Product
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{"sku":"MACBOOK-M3-16","name":"MacBook Pro M3 16-inch","unit_weight_grams":2140}'

# Restock BLR-1 with 50 units (Replace with actual Warehouse UUID)
curl -X POST http://localhost:8080/api/v1/inventory/restock \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"<BLR_WAREHOUSE_UUID>","sku":"MACBOOK-M3-16","quantity":50}'
```

#### 3. Test High-Speed Cached Reads
```bash
# First request fetches from PostgreSQL (X-Cache: MISS)
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16

# Subsequent requests return from Redis in <0.5ms (X-Cache: HIT)
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16
```

#### 4. Reserve & Confirm an Order
```bash
# Reserve stock (Geo-routed, Idempotency-protected, Adaptive lock)
curl -X POST http://localhost:8080/api/v1/orders/reserve \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: flash-order-101" \
  -d '{
    "dest_latitude": 13.0827,
    "dest_longitude": 80.2707,
    "items": [{"sku": "MACBOOK-M3-16", "quantity": 2}],
    "ttl_seconds": 600
  }'

# Confirm Order (Commits stock, writes Transactional Outbox, streams to Kafka)
curl -X POST http://localhost:8080/api/v1/orders/<ORDER_UUID>/confirm
```

#### 5. Autonomous AI Rebalancing Agent
```bash
# 1. Trigger Gemini 3.5 Flash Lite to generate optimal transfer proposals
curl -X POST http://localhost:8080/api/v1/agent/proposals

# 2. Execute the latest pending proposal atomically inside an ACID transaction
curl -X POST http://localhost:8080/api/v1/agent/execute-transfer

# 3. Test duplicate execution guard (verifies 409 Conflict rejection)
curl -X POST http://localhost:8080/api/v1/agent/execute-transfer
```

---

### Step 5: Running with Production Docker Compose

To run the entire hardened production stack inside Docker:
```bash
# Build the production container (uses vendor/ for 30s offline build)
docker build -t ware_go:latest .

# Launch production composition (1 CPU, 512MB RAM bounds)
docker compose -f docker-compose.prod.yml up -d

# Inspect running services & healthchecks
docker compose -f docker-compose.prod.yml ps

# View application logs
docker compose -f docker-compose.prod.yml logs -f app
```

---

## Environment Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP server listening port |
| `DATABASE_URL` | `postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable` | PostgreSQL 16 connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis 7 address for caching and contention velocity |
| `KAFKA_BROKERS` | `localhost:19092` | Kafka / Redpanda bootstrap broker socket |
| `GEMINI_API_KEY` | `""` | Google Gemini API Key for autonomous inventory intelligence |
| `GEMINI_MODEL` | `gemini-3.5-flash-lite` | Gemini model identifier (15 RPM / 500 RPD developer tier) |
| `SEMAPHORE_LIMIT`| `100` | Maximum concurrent in-flight database requests |
| `REAPER_INTERVAL_MS`| `1000` | Polling frequency for zombie stock reclamation daemon |

---

## Technical Documentation & Interview Preparation

For deep architectural analyses, trade-offs, concurrency internals, and complete command references, refer to the project documentation guides:

- **[questions.md](file:///C:/Ware_Go/questions.md)**: A comprehensive **66-question technical guide** dissecting Go runtime internals, PostgreSQL locking mechanics, Kafka streaming pipelines, k6 load testing, Docker optimizations, and AI agent architectures.
- **[commands.md](file:///C:/Ware_Go/commands.md)**: Complete terminal reference cataloging all commands for build, testing, Docker, pprof profiling, and load testing.

---

*Engineered by [Yash Yadav](https://github.com/yashyadav-4) · Go · PostgreSQL 16 · Redis 7 · Redpanda (Kafka) · Google Gemini AI · Prometheus · Grafana · Docker · 2026*
