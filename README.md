# Ware_Go — Multi-Warehouse Inventory & Order Fulfillment Engine

> A high-concurrency backend system in **Go** and **PostgreSQL** that manages inventory across distributed warehouses and processes thousands of concurrent order reservations — **without ever overselling a single unit**. Engineered with ACID transactions, pluggable concurrency strategies, transactional outbox streaming, background stock reapers, and full Prometheus/Grafana observability.

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-FF6B35?logo=apachekafka)](https://redpanda.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-v2.54-E6522C?logo=prometheus)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-11.2-F46800?logo=grafana)](https://grafana.com)
[![k6](https://img.shields.io/badge/k6-Load_Testing-7D64FF?logo=k6)](https://k6.io)

---

## Table of Contents

- [What It Does](#what-it-does)
- [System Architecture](#system-architecture)
- [Design Decisions & Engineering Depth](#design-decisions--engineering-depth)
  - [1. Adaptive Concurrency Strategy (Strategy Pattern)](#1-adaptive-concurrency-strategy-strategy-pattern)
  - [2. Transactional Outbox Pattern](#2-transactional-outbox-pattern)
  - [3. Geo-Aware Warehouse Routing](#3-geo-aware-warehouse-routing)
  - [4. Reservation Reaper (Zombie Stock Reclamation)](#4-reservation-reaper-zombie-stock-reclamation)
  - [5. Idempotency Middleware](#5-idempotency-middleware)
  - [6. Semaphore-Based Load Shedding](#6-semaphore-based-load-shedding)
  - [7. Redis Read-Through Cache with Write Invalidation](#7-redis-read-through-cache-with-write-invalidation)
  - [8. Deadlock-Immune Row Ordering](#8-deadlock-immune-row-ordering)
  - [9. Defensive Multi-Layer Data Integrity](#9-defensive-multi-layer-data-integrity)
  - [10. Real-Time Observability & Profiling (Prometheus + Grafana + pprof)](#10-real-time-observability--profiling-prometheus--grafana--pprof)
- [Scalability & Performance](#scalability--performance)
- [Database Schema Design](#database-schema-design)
- [Testing & Verification Suite](#testing--verification-suite)
  - [Unit & Integration Tests (Go)](#unit--integration-tests-go)
  - [Automated k6 Load & Chaos Suite](#automated-k6-load--chaos-suite)
- [API Reference](#api-reference)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Environment Variables](#environment-variables)

---

## What It Does

Ware_Go solves the core problem behind e-commerce flash sales, high-demand product drops, and multi-warehouse logistics networks: **how do you allow thousands of customers to race for the exact same limited inventory — simultaneously, across multiple geographic nodes, with zero overselling, zero deadlocks, and immediate recovery from failures?**

It handles the complete order lifecycle:

1. **Route** incoming orders to optimal warehouse(s) via a weighted cost function balancing customer proximity (Haversine distance) and inventory depth.
2. **Reserve** stock inside strict ACID database transactions using a pluggable, adaptive concurrency engine (`Atomic`, `Optimistic CAS with Jitter`, or `Pessimistic FOR UPDATE`).
3. **Confirm** or **Cancel** orders with zero-loss rollback, audit ledger writes, and status state machine validation.
4. **Reclaim** abandoned or expired cart reservations automatically through an autonomous background reaper daemon (`SLA < 5s`).
5. **Stream** fulfillment and cancellation events to downstream microservices and 3PL carriers via the **Transactional Outbox Pattern** — guaranteeing zero lost events even if Kafka crashes mid-flight.
6. **Observe & Profile** system vitals in real time through fine-grained Prometheus metrics, pre-configured Grafana dashboards, and live Go pprof endpoints.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Client / k6 Load Generator                     │
└──────────────────────────────┬──────────────────────────────┘
                               │
               ┌───────────────▼──────────────┐
               │         chi Router           │
               │  ┌──────────┐ ┌───────────┐  │
               │  │Semaphore │ │Idempotency│  │
               │  │  (100    │ │  (SHA-256 │  │
               │  │ in-flight│ │  finger-  │  │
               │  │ admission│ │  printing)│  │
               │  └──────────┘ └───────────┘  │
               │  ┌──────────┐ ┌───────────┐  │
               │  │ /metrics │ │ /debug/   │  │
               │  │(Prometh.)│ │   pprof   │  │
               │  └──────────┘ └───────────┘  │
               └───────────────┬──────────────┘
                               │
         ┌─────────────────────▼──────────────────────┐
         │               Service Layer                │
         │  • RoutingService   → cost(distance, stock)│
         │  • ReservationService → ACID Orchestration │
         └─────────────┬──────────────────────┬───────┘
                       │                      │
        ┌──────────────▼──────┐        ┌──────▼───────────────────┐
        │  Adaptive Strategy  │        │      PostgreSQL 16       │
        │ ┌─────────────────┐ │        │                          │
        │ │   Pessimistic   │ │        │  • warehouses, products  │
        │ │ (FOR UPDATE)    │ │        │  • inventory_items       │
        │ ├─────────────────┤ │        │  • orders & order_items  │
        │ │ Optimistic CAS  │ │        │  • transactional outbox  │
        │ │(Version+Jitter) │ │        │  • inventory_audit_log   │
        │ ├─────────────────┤ │        │  • idempotency_key store │
        │ │  Atomic Cond.   │ │        └────────────┬─────────────┘
        │ │(WHERE avail>=N) │ │                     │
        │ └─────────────────┘ │        ┌────────────▼─────────────┐
        └──────────────┬──────┘        │         Redis 7          │
                       │               │  • Stock Read Cache      │
                       └──────────────►│  • SKU Contention Rates  │
                                       └────────────┬─────────────┘
                                                    │
        ┌───────────────────────────────────────────┼────────────────────────────────────────┐
        │                                           │                                        │
┌───────▼─────────────┐                   ┌─────────▼───────────┐                  ┌─────────▼──────────────┐
│    Outbox Relay     │                   │    Reaper Daemon    │                  │  Logistics Consumer    │
│  (500ms poll,       │                   │  (1s poll,          │                  │  (3PL carrier dispatch,│
│   SKIP LOCKED,      │                   │   SKIP LOCKED,      │                  │   consumer group,      │
│   batch 100)        │                   │   auto-rollback)    │                  │   in-memory dedup)     │
└───────┬─────────────┘                   └─────────────────────┘                  └────────────────────────┘
        │
        ▼
┌─────────────────────┐                   ┌─────────────────────┐                  ┌────────────────────────┐
│  Redpanda / Kafka   │                   │  Prometheus v2.54   │                  │      Grafana 11.2      │
│  Topic:             │                   │  Scrapes :8080/     │                  │  Pre-built Dashboard:  │
│  orders.events      │                   │  metrics every 1s   │─────────────────►│  Throughput, Latency,  │
└─────────────────────┘                   └─────────────────────┘                  │  Slots, Worker Rates   │
                                                                                   └────────────────────────┘
```

---

## Design Decisions & Engineering Depth

### 1. Adaptive Concurrency Strategy (Strategy Pattern)

The engine implements **three distinct database locking strategies** behind a unified `ReservationStrategy` interface and dynamically switches between them at runtime:

| Strategy | Implementation Details | Contention Sweet Spot |
|---|---|---|
| **Pessimistic** | `SELECT ... FOR UPDATE` acquires an exclusive row lock prior to reading available inventory. | **High Contention** (flash sales, hot SKUs). Completely prevents CAS retry storms. |
| **Optimistic** | Read-then-CAS using a monotonic `version` counter: `UPDATE ... WHERE version = ?`. If a version collision occurs, it retries with exponential backoff plus full randomized jitter (`5ms * 2^attempt + rand(0-10ms)`). | **Low to Moderate Contention**. Zero lock wait overhead for non-overlapping inventory baskets. |
| **Atomic Conditional** | Single-statement execution: `UPDATE ... WHERE available_qty >= N`. Eliminates separate read round-trips. | **Single-Item Fast Path**. Highest throughput under high-velocity single-item purchases. |

The `AdaptiveStrategy` coordinates automated selection:
- **Fast Path Selection**: Single-item orders are routed directly to the zero-lock Atomic strategy.
- **Dynamic Contention Detection**: Tracks request frequencies per SKU in Redis using sliding-window counters (`INCR` + `EXPIRE 1s`). When request velocity for a SKU hits the contention threshold ($\ge 10\text{ req/s}$), it immediately escalates multi-item orders to Pessimistic locking.
- **Context Overrides**: Specific locking strategies can be pinned on demand via `context.WithValue(ctx, StrategyOverrideKey, "pessimistic")`.

### 2. Transactional Outbox Pattern

To avoid dual-write distributed transaction failures between PostgreSQL and Kafka, domain events are written to an `outbox` table within the **same database transaction** as the inventory state mutation:

- **Transactional Atomicity**: Stock reservation/confirmation and the event insert succeed or roll back together.
- **Lock-Free Concurrency**: The background relay fetches batches using `SELECT ... FOR UPDATE SKIP LOCKED`. Multiple relay instances partition the outbox table cleanly without blocking each other.
- **Stale Lease Recovery**: If an outbox worker crashes mid-publish, rows stuck in `PROCESSING` status for $>30\text{s}$ are safely reclaimed back to `PENDING`.
- **Circuit Breaker / Dead Letter**: Failing publishes increment an `attempts` counter. After 5 retries, status becomes `FAILED` to prevent poison pills from blocking the pipeline.

### 3. Geo-Aware Warehouse Routing

Orders are allocated across warehouse nodes using a **weighted cost function** that balances transit proximity and network stock preservation:

$$\text{cost} = 0.7 \times \left(\frac{\text{distance}}{\text{max\_distance}}\right) + 0.3 \times (1.0 - \text{stock\_ratio})$$

- **Distance**: Computed via spherical geometry with the Haversine formula on Earth's radius ($R = 6,371\text{ km}$).
- **Stock Ratio**: Ratio of warehouse available quantity over total network available quantity.
- **Multi-Warehouse Split Fill**: When no single warehouse holds sufficient inventory, the allocator sorts candidates by cost ascending and splits the order across the closest adequately-stocked nodes.

### 4. Reservation Reaper (Zombie Stock Reclamation)

Abandoned checkouts lock inventory away from paying customers. The **Reaper** daemon runs as an autonomous background worker:

- Scans for orders with `status = 'RESERVED'` and `expires_at < NOW()` on a 1-second interval using `FOR UPDATE SKIP LOCKED`.
- Restores stock atomically: `available_qty += quantity, reserved_qty -= quantity`.
- Writes immutable `ROLLBACK` audit records.
- Transitions order status to `EXPIRED`.
- **SLA Guarantee**: Verified in automated testing and k6 chaos tests to reclaim abandoned stock within 3 seconds (well below the 5-second SLA target).

### 5. Idempotency Middleware

Prevents duplicate orders caused by client retries, network blips, or double submissions:

1. Requires an `Idempotency-Key` header on mutating endpoints.
2. Generates a SHA-256 checksum of the request body payload.
3. If key exists with different payload $\to$ rejects with `422 Unprocessable Entity`.
4. If key exists with state `IN_PROGRESS` $\to$ rejects with `409 Conflict`.
5. If key exists with state `COMPLETED` $\to$ immediately returns stored status and JSON body with `X-Idempotent-Replay: true`.
6. New requests are registered in an `IN_PROGRESS` state, executed through a `responseRecorder`, and committed with the status and payload in a 5-second bounded completion context.

### 6. Semaphore-Based Load Shedding

Prevents database pool exhaustion under extreme traffic surges:

```go
select {
case sem <- struct{}{}:    // Slot acquired: increments metrics.SemaphoreActiveSlots
    defer func() { <-sem; metrics.SemaphoreActiveSlots.Dec() }()
    next.ServeHTTP(w, r)
case <-time.After(wait):   // Timed out waiting for slot (100ms)
    metrics.SemaphoreRejectionsTotal.Inc()
    w.Header().Set("Retry-After", "1")
    w.WriteHeader(http.StatusServiceUnavailable) // 503
case <-r.Context().Done(): // Client disconnected
    return
}
```

- Bounded capacity: 100 concurrent in-flight requests.
- Max queue wait: 100ms. Overflows immediately receive `503 Service Unavailable` with `Retry-After: 1`.

### 7. Redis Read-Through Cache with Write Invalidation

- **Read Path**: `GET /api/v1/inventory/{sku}` checks Redis first. On hit, writes cached JSON directly to the wire with `X-Cache: HIT`. On miss, queries PostgreSQL, marshals response, caches with a 5-second TTL, and returns `X-Cache: MISS`.
- **Write Path**: Restock operations (`POST /api/v1/inventory/restock`) invalidate the cache immediately upon transaction commit (`DEL inventory:<sku>`).

### 8. Deadlock-Immune Row Ordering

To eliminate database deadlocks during multi-item, cross-warehouse transactions, every transactional mutation sorts all items by `(warehouse_id ASC, sku ASC)` prior to acquiring locks:

```go
sort.Slice(items, func(i, j int) bool {
    cmp := bytes.Compare(items[i].WarehouseID.Bytes[:], items[j].WarehouseID.Bytes[:])
    if cmp != 0 {
        return cmp < 0
    }
    return items[i].Sku < items[j].Sku
})
```

Applied consistently across:
- `ReserveOrder`
- `ConfirmOrder`
- `CancelOrder`
- `ReapExpiredOrders`

### 9. Defensive Multi-Layer Data Integrity

The system guarantees inventory correctness through three decoupled layers of defense:

```
[Layer 1: Application Logic]
  • Abort-before-lock checks
  • Pre-condition checks: available_qty < req.Quantity → return ErrInsufficientStock
              │
              ▼
[Layer 2: SQL Conditional Updates]
  • UPDATE inventory_items SET ... WHERE warehouse_id = $1 AND sku = $2 AND available_qty >= $3
  • Zero rows updated if stock was depleted concurrently
              │
              ▼
[Layer 3: Database Engine Constraint]
  • CONSTRAINT chk_available_non_negative CHECK (available_qty >= 0)
  • CONSTRAINT chk_reserved_non_negative CHECK (reserved_qty >= 0)
  • Enforced by PostgreSQL kernel with SQLSTATE 23514 (check_violation)
```

### 10. Real-Time Observability & Profiling (Prometheus + Grafana + pprof)

The engine includes built-in observability with zero external agent dependencies:

#### Prometheus Metrics Vector (`/metrics`)
- `warego_reservation_duration_seconds`: Histogram measuring end-to-end reservation transaction latency partitioned by `strategy` (`adaptive`, `pessimistic`, `optimistic`, `atomic`) and `status` (`success`, `out_of_stock`, `error`).
- `warego_reservations_total`: Counter tracking total attempts by strategy and outcome.
- `warego_semaphore_active_slots`: Gauge reporting active in-flight requests (0–100).
- `warego_semaphore_rejections_total`: Counter tracking shed requests (503).
- `warego_reaper_reclaimed_total`: Counter tracking zombie orders reclaimed by the daemon.
- `warego_reaper_cycle_duration_seconds`: Histogram tracking reaper execution time.
- `warego_outbox_events_total`: Counter partitioned by `status` (`published`, `failed`) and `event_type`.

#### Pre-Provisioned Grafana Dashboard (`http://localhost:3000`)
- **Reservation Throughput (RPS)**: Real-time traffic broken down by outcome status.
- **Active Semaphore Slots**: Visual gauge (0–100) with green/yellow/red saturation thresholds.
- **Strategy Breakdown (RPS)**: Live distribution of traffic handled by each locking strategy.
- **Latency Percentiles (p50 / p95 / p99)**: Sub-second SLA tracking across all transactions.
- **Background Workers Activity**: Real-time reaper reclamation and Kafka outbox dispatch rates.

#### Live Go Runtime Profiling (`/debug/pprof/`)
Mounted directly via Chi middleware profiler for diagnosing locks, memory allocation, and CPU bottlenecks under high concurrency:
- `/debug/pprof/goroutine` — inspect active goroutines and lock waits
- `/debug/pprof/profile` — CPU profiling under load
- `/debug/pprof/heap` — memory allocations
- `/debug/pprof/mutex` — lock contention analysis

---

## Scalability & Performance

| Parameter | Configuration & Implementation |
|---|---|
| **Database Pool** | `pgxpool` configured with `MinConns = 5`, `MaxConns = 25`, `MaxConnIdleTime = 5m`. |
| **Admission Control** | Channel semaphore caps in-flight operations at 100 with a 100ms queue wait before load shedding. |
| **Worker Scaling** | Both Reaper and Outbox Relay use `FOR UPDATE SKIP LOCKED`, allowing horizontal scaling across N worker nodes without lock conflicts. |
| **Batch Drainage** | Reaper and Outbox Relay execute up to 50 batches (100 rows/batch) per tick to clear traffic spikes smoothly. |
| **Partial Indexes** | `idx_inventory_sku_avail` indexes only `available_qty > 0`. `idx_orders_expired` indexes only `status = 'RESERVED'`. |
| **Memory Bound** | `http.MaxBytesReader` caps all incoming JSON request bodies at 1MB to protect against payload memory exhaustion. |
| **Connection Timeouts** | `ReadTimeout: 5s`, `WriteTimeout: 10s`, `IdleTimeout: 120s`. |

---

## Database Schema Design

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
┌──────────────────┐       ┌──────────────────┐
│      outbox      │       │ idempotency_key  │
│──────────────────│       │──────────────────│
│ id (UUID PK)     │       │ key (VARCHAR PK) │
│ aggregate_type   │       │ request_hash     │
│ aggregate_id     │       │ state (ENUM)     │
│ event_type       │       │ response_status  │
│ payload (JSONB)  │       │ response_body    │
│ status (ENUM)    │       │ created_at       │
│ attempts (INT)   │       │ completed_at     │
│ locked_at/by     │       └──────────────────┘
└──────────────────┘
```

---

## Testing & Verification Suite

### Unit & Integration Tests (Go)

The native Go test suite verifies business logic, database constraints, and concurrency guarantees:

```bash
# Unit tests
go test -v ./pkg/geo/...
go test -v ./internal/api/middleware/...

# Integration tests (requires Docker services)
go test -v ./test/integration/...

# Race condition detection
go test -race ./...

# Concurrency & throughput benchmarks
go test -bench=. -benchmem ./test/integration/...
```

#### Test Coverage Summary
- `TestInventoryConstraints`: Verifies restock logic and verifies that PostgreSQL rejects negative stock with SQLSTATE `23514`.
- `TestConcurrentReservationZeroOversell`: **1,000 goroutines** race for 100 available units. Asserts exactly 100 successes, 900 rejections, `available_qty = 0`, and `reserved_qty = 100`.
- `TestOrderLifecycle_Confirm`: Full Reserve $\to$ Confirm lifecycle. Asserts reserved quantity commits to 0 and order status reaches `CONFIRMED`.
- `TestOrderLifecycle_Cancel`: Full Reserve $\to$ Cancel lifecycle. Asserts full restoration of available stock and status `CANCELLED`.
- `TestOrderLifecycle_StateTransitions`: Confirms that invalid state transitions (e.g. double confirmation or cancelling confirmed orders) are strictly rejected.
- `TestOutboxRelay_PublishPendingEvents`: Verifies transactional outbox event creation, Kafka writer dispatch, and transition to `PUBLISHED`.
- `TestOutboxRelay_RetryOnFailure`: Simulates broker network timeouts, verifying retry counter increments without data loss.
- `TestOutboxRelay_ReclaimStaleLeases`: Verifies that events locked by crashed workers are reclaimed.
- `TestReaper_ReclaimExpiredOrder`: Tests automatic stock restoration when order TTL expires.
- `TestReaper_SLA_Within5Seconds`: Live daemon test verifying expired reservations are restored within 3 seconds.

---

### Automated k6 Load & Chaos Suite

Located in `test/k6/`, these automated scenarios simulate realistic production flash sales, deadlocks, and broker failures:

```bash
# 1. Zero Oversell Flash Sale Spike (500 VUs, 2,000 iterations against 100 units)
k6 run test/k6/s1_zero_oversell.js

# 2. Zombie Stock Reclamation SLA (< 5s target)
k6 run test/k6/s2_zombie_stock.js

# 3. Cross-Warehouse Deadlock Immunity Stress Test (200 VUs, 1,000 orders)
k6 run test/k6/s3_deadlock.js

# 4. Sustained Throughput & Latency (500 RPS constant rate for 20s)
k6 run test/k6/s4_latency.js

# 5. Outbox Event Streaming Pipeline Verification
k6 run test/k6/s5_outbox_chaos.js

# 6. High vs Low Contention Locking Strategy Benchmark
k6 run test/k6/s6_strategy_benchmark.js
```

| Scenario | Objective & Workload | Success Criteria / Thresholds |
|---|---|---|
| **S1: Zero Oversell** | 500 VUs generate 2,000 requests racing for 100 units of a hot flash-sale SKU. | `successful_reservations == 100`, zero oversell. |
| **S2: Zombie Stock** | 10 VUs create 50 abandoned orders with 1-second TTL. Background reaper reclaims stock. | `p(95) reclaim latency < 5000ms`. |
| **S3: Deadlock Immunity** | 200 VUs place 1,000 orders locking items across 2 warehouses in reverse orders (WH1+WH2 vs WH2+WH1). | `deadlock_errors == 0`. |
| **S4: Latency & Throughput** | Constant arrival rate of 500 RPS sustained over 20s (up to 300 VUs). | `http_req_duration p(95) < 1000ms`. |
| **S5: Outbox Pipeline** | End-to-end Reserve $\to$ Confirm lifecycle streaming events to Redpanda. | 100% events delivered to broker. |
| **S6: Strategy Benchmark** | Compares hot SKU contention (50 VUs) against catalog distribution (50 VUs). | Validates automated strategy escalation. |

---

## API Reference

All application endpoints are served under `/api/v1` (with system endpoints at root):

### System & Telemetry
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Deep health check (verifies PostgreSQL and Redis connectivity). |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint. |
| `GET` | `/debug/pprof/*` | Go runtime profiling index (goroutine, heap, cpu, mutex). |

### Warehouses
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/warehouses` | Register a new warehouse with geographic coordinates. |
| `GET` | `/api/v1/warehouses` | List all active warehouse facilities. |

### Products
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/products` | Create a catalog SKU with name and unit weight. |
| `GET` | `/api/v1/products` | List all catalog products. |
| `GET` | `/api/v1/products/{sku}` | Retrieve product details by SKU. |

### Inventory
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/inventory/restock` | Restock SKU inventory in a warehouse (ACID tx + audit log + cache invalidation). |
| `GET` | `/api/v1/inventory/{sku}` | Query stock summary across all warehouses (`X-Cache: HIT` / `MISS`). |

### Orders & Reservations
| Method | Endpoint | Description | Headers & Parameters |
|---|---|---|---|
| `POST` | `/api/v1/orders/reserve` | Reserve multi-item order across optimal warehouses. | Header: `Idempotency-Key` (required)<br>Payload: `{"dest_latitude":..., "dest_longitude":..., "items":[...], "ttl_seconds": 600}` |
| `GET` | `/api/v1/orders/{id}` | Fetch order details along with assigned warehouse line items. | Path: `id` (UUID) |
| `POST` | `/api/v1/orders/{id}/confirm` | Confirm order: commits reserved inventory, generates `OrderFulfilled` outbox event. | Path: `id` (UUID) |
| `POST` | `/api/v1/orders/{id}/cancel` | Cancel order: rolls back reserved inventory, generates `OrderCancelled` outbox event. | Path: `id` (UUID) |

---

## Tech Stack

| Component | Technology | Version | Rationale |
|---|---|---|---|
| **Language** | Go | 1.27 | Native concurrency primitives (goroutines, channels), low runtime overhead, predictable garbage collection. |
| **HTTP Router** | chi | v5.5.0 | Lightweight, idiomatic router with zero allocation overhead and composable middleware. |
| **Database** | PostgreSQL | 16-alpine | Row-level locking (`FOR UPDATE SKIP LOCKED`), strict ACID guarantees, and JSONB support. |
| **Database Driver** | pgx / pgxpool | v5.12.0 | High-performance native PostgreSQL wire protocol driver with optimized connection pooling. |
| **Query Generator** | sqlc | v1.31.1 | Compiles raw SQL queries into type-safe, boilerplate-free Go code with zero runtime reflection. |
| **Cache & Counters** | Redis | 7-alpine | In-memory key-value store for stock read caching and sliding-window contention rate limiting. |
| **Message Broker** | Redpanda (Kafka) | v24.1.8 | High-throughput distributed log for reliable downstream event streaming. |
| **Kafka Client** | kafka-go | v0.4.51 | Native Go client supporting least-bytes partition balancing and consumer groups. |
| **Metrics** | Prometheus | v2.54.1 | Time-series metrics engine scraping latency histograms and counters. |
| **Dashboards** | Grafana | 11.2.0 | Real-time telemetry dashboard pre-configured via automated file provisioning. |
| **Load Testing** | k6 | Latest | Developer-centric load generator for high-VU concurrency, deadlock, and chaos simulations. |

---

## Project Structure

```
Ware_Go/
├── cmd/
│   └── server/
│       └── main.go                         # Server bootstrap: pools, Kafka, background daemons, graceful shutdown
├── deploy/
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   └── ware_go_dashboard.json      # Pre-built Grafana performance & concurrency dashboard
│   │   └── provisioning/
│   │       ├── dashboards/dashboards.yml   # Automatic dashboard provisioning
│   │       └── datasources/prometheus.yml  # Prometheus data source configuration
│   └── prometheus/
│       └── prometheus.yml                  # Prometheus scrape config targeting host:8080/metrics
├── internal/
│   ├── api/
│   │   ├── router.go                       # Route registration & handler injection
│   │   ├── inventory.go                    # Restock (ACID tx) & stock summary (cache-aside)
│   │   ├── orders.go                       # Order reservation, confirmation, cancellation & lookup
│   │   ├── products.go                     # Product catalog CRUD
│   │   ├── warehouses.go                   # Warehouse facilities CRUD
│   │   ├── response.go                     # JSON serialization & 1MB body limit enforcement
│   │   └── middleware/
│   │       ├── idempotency.go              # SHA-256 idempotency check with response replay
│   │       ├── semaphore.go                # Admission control & load shedding (100 concurrent slots)
│   │       └── semaphore_test.go           # Semaphore unit & timeout tests
│   ├── config/
│   │   └── config.go                       # Environment variable loader
│   ├── metrics/
│   │   └── metrics.go                      # Prometheus histograms, counters, and gauges
│   ├── repository/
│   │   ├── db/                             # sqlc-generated type-safe queries & models
│   │   └── queries/                        # Handwritten raw SQL queries
│   │       ├── inventory.sql
│   │       ├── orders.sql
│   │       ├── outbox.sql
│   │       ├── idempotency.sql
│   │       ├── products.sql
│   │       └── warehouses.sql
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
│   └── 002_outbox.sql                      # Schema: outbox table with status partial indexes
├── pkg/
│   └── geo/
│       ├── haversine.go                    # Great-circle distance calculations
│       └── haversine_test.go               # Distance unit tests
├── test/
│   ├── integration/
│   │   ├── inventory_test.go               # PostgreSQL CHECK constraint verification
│   │   ├── reservation_test.go             # 1,000-goroutine zero-oversell proof
│   │   ├── order_lifecycle_test.go         # Full reserve -> confirm -> cancel lifecycle tests
│   │   ├── outbox_test.go                  # Outbox relay, retry, and lease reclamation tests
│   │   ├── reaper_test.go                  # Zombie stock reclamation SLA tests
│   │   └── benchmark_test.go               # Strategy throughput benchmarks
│   └── k6/
│       ├── s1_zero_oversell.js             # 500 VU flash sale spike test (100 units)
│       ├── s2_zombie_stock.js              # Zombie stock reclamation SLA test
│       ├── s3_deadlock.js                  # Cross-warehouse reverse locking deadlock test
│       ├── s4_latency.js                   # 500 RPS sustained throughput latency test
│       ├── s5_outbox_chaos.js              # Outbox streaming reliability test
│       └── s6_strategy_benchmark.js        # High vs low contention strategy benchmark
├── docker-compose.yml                      # Postgres, Redis, Redpanda, Prometheus, Grafana
├── sqlc.yml                                # sqlc generator configuration
└── go.mod                                  # Module definition & dependencies
```

---

## Getting Started

### Prerequisites
- **Go 1.27+**
- **Docker Desktop**
- **k6** (optional, for running load tests)

### 1. Clone & Start Infrastructure
```bash
git clone https://github.com/yashyadav-4/ware_go.git
cd ware_go

# Start all infrastructure services (Postgres, Redis, Redpanda, Prometheus, Grafana)
docker compose up -d
```

### 2. Apply Database Migrations
```bash
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/001_init.sql
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/002_outbox.sql
```

### 3. Run the Server
```bash
go run cmd/server/main.go
```

The server initializes on port `8080`, launches the background Reaper and Outbox Relay daemons, and begins streaming to Kafka.

### 4. Verify Services
- **Health Check**: `curl http://localhost:8080/healthz`
- **Prometheus UI**: `http://localhost:9090`
- **Grafana Dashboard**: `http://localhost:3000` (Login: `admin` / `admin` $\to$ browse to `Ware_Go — Concurrency & Performance Engine`)
- **pprof Profiler**: `http://localhost:8080/debug/pprof/`

### 5. Quick Walkthrough
```bash
# 1. Create a warehouse
curl -X POST http://localhost:8080/api/v1/warehouses \
  -H "Content-Type: application/json" \
  -d '{"code":"BLR-1","name":"Bangalore Hub","latitude":12.9716,"longitude":77.5946}'

# 2. Register a product
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{"sku":"MACBOOK-M3-16","name":"MacBook Pro M3 16-inch","unit_weight_grams":2140}'

# 3. Restock inventory (ACID transaction + audit log + cache invalidation)
curl -X POST http://localhost:8080/api/v1/inventory/restock \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"<INSERT_WAREHOUSE_UUID>","sku":"MACBOOK-M3-16","quantity":50}'

# 4. Read stock (First request: X-Cache: MISS, Second request: X-Cache: HIT)
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16

# 5. Reserve an order (Idempotent, Geo-routed, Adaptive Lock)
curl -X POST http://localhost:8080/api/v1/orders/reserve \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: flash-order-001" \
  -d '{
    "dest_latitude": 28.6139,
    "dest_longitude": 77.2090,
    "items": [{"sku": "MACBOOK-M3-16", "quantity": 2}],
    "ttl_seconds": 600
  }'

# 6. Confirm the order (Commits stock and writes to Outbox -> published to Kafka)
curl -X POST http://localhost:8080/api/v1/orders/<INSERT_ORDER_UUID>/confirm
```

---

## Running Tests

### Unit & Integration Suite
```bash
# Run all unit tests
go test -v ./pkg/geo/... ./internal/api/middleware/...

# Run all integration tests against Docker
go test -v ./test/integration/...

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem ./test/integration/...
```

### k6 Concurrency & Chaos Suite
Make sure the server and Docker containers are running:

```bash
# S1: Flash Sale Spike (500 VUs racing for 100 items -> verifies zero oversell)
k6 run test/k6/s1_zero_oversell.js

# S2: Zombie Stock Reclamation SLA (< 5s target)
k6 run test/k6/s2_zombie_stock.js

# S3: Cross-Warehouse Deadlock Immunity (200 VUs, reverse lock ordering)
k6 run test/k6/s3_deadlock.js

# S4: Sustained Throughput Latency (500 RPS)
k6 run test/k6/s4_latency.js

# S5: Transactional Outbox Pipeline & Chaos
k6 run test/k6/s5_outbox_chaos.js

# S6: Adaptive Strategy Benchmark (Hot vs Cold SKUs)
k6 run test/k6/s6_strategy_benchmark.js
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server listening port |
| `DATABASE_URL` | `postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable` | PostgreSQL connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis server address |
| `KAFKA_BROKERS` | `localhost:19092` | Kafka / Redpanda broker address |
| `GEMINI_API_KEY` | *(Optional)* | Gemini API Key for autonomous inventory intelligence |

---

*Built by [Yash Yadav](https://github.com/yashyadav-4) · Go · PostgreSQL · Redis · Kafka · Prometheus · Grafana · 2026*
