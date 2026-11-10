# Ware_Go — Multi-Warehouse Inventory & Order Fulfillment Engine

> A high-concurrency backend system in **Go** and **PostgreSQL** that manages inventory across distributed warehouses and processes thousands of concurrent order reservations — **without ever overselling a single unit**.

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-FF6B35?logo=apachekafka)](https://redpanda.com)

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
  - [9. Defensive Data Integrity](#9-defensive-data-integrity)
- [Scalability & Performance](#scalability--performance)
- [Database Schema Design](#database-schema-design)
- [Testing Strategy](#testing-strategy)
- [API Reference](#api-reference)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Environment Variables](#environment-variables)

---

## What It Does

Ware_Go solves the core problem behind every e-commerce flash sale, limited-edition drop, and multi-warehouse fulfillment system: **how do you let thousands of customers race for the same limited inventory — simultaneously, correctly, and without selling units that don't exist?**

It handles the full order lifecycle:

1. **Route** each order to the optimal warehouse(s) based on customer proximity and stock availability
2. **Reserve** inventory inside strict ACID transactions using an adaptive concurrency strategy
3. **Confirm** or **Cancel** orders with full stock rollback and audit trails
4. **Reclaim** abandoned reservations automatically via a background reaper daemon
5. **Publish** every lifecycle event to downstream systems via the Transactional Outbox pattern — even if Kafka is temporarily down

---

## System Architecture

```
┌──────────────────────────────────────────────┐
│              Client / HTTP                   │
└────────────────────┬─────────────────────────┘
                     │
       ┌─────────────▼──────────────┐
       │      chi Router            │
       │  ┌──────────┐ ┌─────────┐  │
       │  │Semaphore  │ │Idempot. │  │
       │  │  (load    │ │  (SHA-  │  │
       │  │ shedding) │ │  256 +  │  │
       │  │  100 max  │ │ replay) │  │
       │  └──────────┘ └─────────┘  │
       └─────────────┬──────────────┘
                     │
     ┌───────────────▼────────────────┐
     │         Service Layer          │
     │  RoutingService → cost(d,s)    │
     │  ReservationService → ACID tx  │
     └───────────┬───────────┬────────┘
                 │           │
    ┌────────────▼──┐  ┌─────▼──────────┐
    │  Adaptive     │  │  PostgreSQL 16  │
    │  Strategy     │  │                 │
    │ ┌───────────┐ │  │  inventory      │
    │ │Pessimistic│ │  │  orders         │
    │ │(FOR UPDT) │ │  │  outbox         │
    │ ├───────────┤ │  │  audit_log      │
    │ │Optimistic │ │  │  idempotency    │
    │ │(CAS+Retry)│ │  └────────┬────────┘
    │ ├───────────┤ │           │
    │ │  Atomic   │ │  ┌────────▼────────┐
    │ │(WHERE >=) │ │  │  Redis 7        │
    │ └───────────┘ │  │  • read cache   │
    └───────────────┘  │  • contention   │
                       │    rate counter │
                       └────────┬────────┘
                                │
        ┌───────────────────────┼────────────────────┐
        │                       │                    │
┌───────▼────────┐  ┌───────────▼──────┐  ┌──────────▼─────────┐
│  Outbox Relay  │  │   Reaper Daemon  │  │  Kafka Consumer    │
│  (500ms poll,  │  │  (1s poll,       │  │  (3PL logistics    │
│  batch 100,    │  │  batch 100,      │  │   dispatcher,      │
│  SKIP LOCKED)  │  │  SKIP LOCKED,    │  │   dedup by key)    │
│       │        │  │  TTL rollback)   │  └────────────────────┘
│       ▼        │  └──────────────────┘
│  Redpanda/     │
│  Kafka         │
└────────────────┘
```

---

## Design Decisions & Engineering Depth

### 1. Adaptive Concurrency Strategy (Strategy Pattern)

The system implements **three distinct reservation strategies** behind a common `ReservationStrategy` interface and dynamically selects the optimal one at runtime:

| Strategy | Mechanism | Best For |
|---|---|---|
| **Pessimistic** | `SELECT ... FOR UPDATE` — acquires an exclusive row lock before reading | High contention (flash sales, hot SKUs) |
| **Optimistic** | CAS with version column — read, then `UPDATE ... WHERE version = ?` with exponential backoff + jitter | Low-to-moderate contention |
| **Atomic Conditional** | Single `UPDATE ... WHERE available_qty >= ?` — no separate read | Single-item fast path |

The `AdaptiveStrategy` orchestrates selection:
- **Context override**: Callers can force a specific strategy via `context.Value`
- **Single-item optimization**: Automatically routes 1-item orders to the zero-round-trip Atomic strategy
- **Contention detection**: Uses Redis sliding-window counters (`INCR` + `EXPIRE 1s`) per SKU — when the request rate for a SKU exceeds the threshold (10 req/s), it escalates to Pessimistic locking to avoid retry storms

**Why this matters**: Optimistic locking is faster under low contention but degrades under high contention due to retry storms. Pessimistic locking is safe but serializes all requests. The adaptive layer gives you the best of both worlds — and our benchmarks prove each strategy independently enforces zero oversell.

### 2. Transactional Outbox Pattern

Event publishing to Kafka uses the **Transactional Outbox** — a pattern that guarantees exactly-once delivery semantics without distributed transactions:

```
                 ┌── Single ACID Transaction ──┐
                 │                              │
Reserve/Confirm ─┤  UPDATE inventory            │
                 │  INSERT INTO orders          │
                 │  INSERT INTO outbox ◄────────┤── Event written atomically
                 │                              │
                 └──────────────────────────────┘
                             │
                   ┌─────────▼──────────┐
                   │   Outbox Relay     │
                   │ (background poll)  │
                   │  SELECT ... FOR    │
                   │  UPDATE SKIP LOCKED│
                   │  → Kafka publish   │
                   │  → Mark PUBLISHED  │
                   └────────────────────┘
```

**Implementation details**:
- **Batched processing**: Fetches up to 100 pending events per cycle, bounded to 50 batches per tick for back-pressure
- **Stale lease reclamation**: If a relay worker crashes mid-publish, rows locked by `PROCESSING` status for >30 seconds are automatically reclaimed
- **Retry with circuit breaker**: Failed publishes increment an `attempts` counter; after 5 failures the event is moved to `FAILED` status for manual inspection
- **Event types**: `OrderFulfilled`, `OrderCancelled`, `OrderExpired` — each with structured JSON payloads and Kafka headers for consumer routing

### 3. Geo-Aware Warehouse Routing

Orders are routed to the optimal warehouse(s) using a **weighted cost function** that balances proximity and stock depth:

```
cost = 0.7 × (distance / max_distance) + 0.3 × (1 − stock_ratio)
```

- **Distance**: Calculated via the Haversine formula (great-circle distance on Earth's surface)
- **Stock ratio**: Proportion of a warehouse's available stock relative to total network stock
- **Multi-warehouse split-fill**: If no single warehouse has enough stock, the order is split across warehouses in cost-sorted order — the closest/most-stocked warehouse fills first, the remainder cascades

**Why 0.7 / 0.3 weighting?** Shipping costs dominate fulfillment economics. A warehouse 50 km away with 20 units is almost always better than one 500 km away with 200 units. The 30% stock-depth factor prevents draining a single warehouse to zero while a neighboring one sits full.

### 4. Reservation Reaper (Zombie Stock Reclamation)

Abandoned checkouts are a silent killer for inventory systems. A customer adds 5 units to cart, the system reserves them, and the customer walks away — those 5 units are stuck in limbo.

The **Reaper** is a background daemon that:
- Polls every 1 second for `RESERVED` orders whose `expires_at` has passed
- Uses `SELECT ... FOR UPDATE SKIP LOCKED` so multiple reaper instances don't contend on the same rows
- Rolls back inventory atomically: `available_qty += reserved, reserved_qty -= reserved`
- Writes `ROLLBACK` entries to the audit log
- Transitions the order to `EXPIRED` status

**SLA guarantee**: Tests verify that expired reservations are reclaimed within 3 seconds (well under the 5-second SLA target).

### 5. Idempotency Middleware

The reservation endpoint is protected by **database-backed idempotency** that prevents duplicate orders from network retries, double-clicks, or frontend bugs:

1. Client sends `Idempotency-Key` header with each request
2. Middleware computes SHA-256 hash of the request body
3. If the key exists with a different hash → `422 Unprocessable Entity` (payload mismatch)
4. If the key exists with state `IN_PROGRESS` → `409 Conflict` (concurrent processing)
5. If the key exists with state `COMPLETED` → replays the cached response with `X-Idempotent-Replay: true`
6. If new → inserts the key, delegates to the handler, captures the response via a `responseRecorder`, and marks the key `COMPLETED` with the cached response

This ensures **exactly-once semantics at the API level** — even if a client retries the same request 10 times, exactly one order is created.

### 6. Semaphore-Based Load Shedding

The server enforces a **bounded concurrency limit** of 100 in-flight requests using a channel-based semaphore middleware:

```go
select {
case sem <- struct{}{}:    // Acquired slot → process request
case <-time.After(wait):   // Timeout → 503 + Retry-After header
case <-r.Context().Done(): // Client disconnected → early return
}
```

**Why not just use timeouts?** A 60-second request timeout allows 6,000 in-flight requests at 100 RPS. The semaphore caps it at 100, shedding excess load immediately with `503 Service Unavailable` and a `Retry-After` header — allowing load balancers and clients to back off gracefully.

### 7. Redis Read-Through Cache with Write Invalidation

Stock queries use a **cache-aside** pattern:

- **Read path**: Check Redis → on HIT, return immediately with `X-Cache: HIT` header. On MISS, query PostgreSQL, cache the result for 5 seconds, return with `X-Cache: MISS`
- **Write path**: After every restock, the cache key for that SKU is immediately invalidated (`DEL inventory:<sku>`)

The 5-second TTL is deliberately short — in a high-velocity inventory system, stale reads can mislead customers. The TTL absorbs read spikes while the invalidation-on-write ensures restocks are immediately visible.

### 8. Deadlock-Immune Row Ordering

Every multi-row transaction in the system — reservation, confirmation, cancellation, and reaper rollback — sorts rows by `(warehouse_id, sku)` before acquiring locks:

```go
sort.Slice(items, func(i, j int) bool {
    cmp := bytes.Compare(items[i].WarehouseID.Bytes[:], items[j].WarehouseID.Bytes[:])
    if cmp != 0 {
        return cmp < 0
    }
    return items[i].Sku < items[j].Sku
})
```

This deterministic lock ordering **eliminates deadlocks entirely** — no matter which goroutine acquires locks first, they always acquire them in the same global order. This is a textbook technique, but it's consistently applied across every transactional code path.

### 9. Defensive Data Integrity

The system enforces inventory correctness at **three independent layers**:

| Layer | Mechanism | What it prevents |
|---|---|---|
| **Application** | Strategy pre-checks (`available_qty < requested` → reject) | Logical oversell |
| **SQL** | `UPDATE ... WHERE available_qty >= $3` (atomic strategy) | Race-condition oversell |
| **Database** | `CHECK (available_qty >= 0)` constraint on `inventory_items` | Any path to negative stock |

If the application logic has a bug, the SQL WHERE clause catches it. If the SQL has a bug, the PostgreSQL CHECK constraint is the final safety net. **No single failure can produce negative inventory.**

---

## Scalability & Performance

| Dimension | Implementation |
|---|---|
| **Connection pooling** | `pgxpool` with 25 max / 5 min connections, 5-minute idle timeout |
| **Concurrent request limit** | Semaphore middleware caps at 100 in-flight; excess gets `503 + Retry-After` |
| **Batch processing** | Outbox relay and reaper both process up to 100 rows per batch, 50 batches per tick |
| **Lock-free polling** | `FOR UPDATE SKIP LOCKED` on outbox and expired-order queries — multiple workers won't block each other |
| **Horizontal scaling** | Workers (reaper, outbox relay) use `SKIP LOCKED` — safe to run N replicas concurrently |
| **Partial-index scans** | `idx_inventory_sku_avail` filters `available_qty > 0`; `idx_orders_expired` filters `status = 'RESERVED'` |
| **Short-lived cache** | 5-second Redis TTL absorbs read spikes without serving dangerously stale data |
| **Graceful shutdown** | SIGINT/SIGTERM triggers context cancellation → in-flight requests drain → 5-second shutdown deadline |
| **HTTP tuning** | `ReadTimeout: 5s`, `WriteTimeout: 10s`, `IdleTimeout: 120s` — balanced for throughput and resource reclamation |

---

## Database Schema Design

```
┌──────────────┐     ┌──────────────┐     ┌─────────────────┐
│  warehouses  │     │   products   │     │  inventory_items │
│──────────────│     │──────────────│     │─────────────────│
│ id (UUID PK) │◄────│ sku (PK)     │◄────│ warehouse_id FK │
│ code (UNIQ)  │     │ name         │     │ sku FK          │
│ latitude     │     │ weight_grams │     │ available_qty   │
│ longitude    │     └──────────────┘     │ reserved_qty    │
│ is_active    │                          │ version         │
└──────────────┘                          │ CHECK(avail>=0) │
                                          │ CHECK(resrv>=0) │
       ┌──────────────────────────────────│ UNIQUE(wh,sku)  │
       │                                  └─────────────────┘
       │
┌──────▼───────┐     ┌──────────────┐     ┌─────────────────┐
│    orders    │     │ order_items   │     │ inventory_audit  │
│──────────────│     │──────────────│     │     _log         │
│ id (UUID PK) │◄────│ order_id FK  │     │─────────────────│
│ idempotency  │     │ warehouse_id │     │ warehouse_id FK │
│   _key (UNIQ)│     │ sku FK       │     │ sku FK          │
│ customer_id  │     │ quantity     │     │ order_id FK     │
│ status (ENUM)│     └──────────────┘     │ event_type      │
│ dest_lat/lon │                          │ delta_available  │
│ expires_at   │     ┌──────────────┐     │ delta_reserved   │
│ version      │     │    outbox     │     └─────────────────┘
└──────────────┘     │──────────────│
                     │ aggregate_type│     ┌─────────────────┐
                     │ aggregate_id  │     │ idempotency_key │
                     │ event_type   │     │─────────────────│
                     │ payload JSONB│     │ key (PK)        │
                     │ status (ENUM)│     │ request_hash    │
                     │ attempts     │     │ state           │
                     │ locked_at/by │     │ response_status │
                     └──────────────┘     │ response_body   │
                                          └─────────────────┘
```

**Key schema decisions**:
- **Separate `available_qty` and `reserved_qty`** — allows the system to distinguish between "in stock but held" and "actually available", critical for accurate customer-facing counts
- **`version` column on inventory** — enables optimistic concurrency control without requiring row-level locks
- **`idempotency_key` on orders** — prevents duplicate order creation at the database level (UNIQUE constraint)
- **Partial indexes** — `idx_inventory_sku_avail WHERE available_qty > 0` skips empty warehouse rows; `idx_orders_expired WHERE status = 'RESERVED'` makes reaper scans efficient
- **`ON DELETE RESTRICT` on inventory FKs** — prevents accidental warehouse or product deletion while inventory exists
- **Order status as ENUM** — `RESERVED → CONFIRMED | CANCELLED | EXPIRED` — enforced by application-level state machine

---

## Testing Strategy

The project has **4 test tiers** covering correctness, concurrency safety, and performance:

### Unit Tests
| Test | What it proves |
|---|---|
| `TestHaversineKm` | Haversine distance calculation: identical points → 0 km, Delhi→Jaipur ≈ 238 km ± 10 km |
| `TestSemaphore_PassThrough` | Normal requests pass through the semaphore |
| `TestSemaphore_QueueWaitSuccess` | Blocked requests acquire released slots |
| `TestSemaphore_TimeoutRejection` | Excess requests get `503` with `Retry-After` header |
| `TestSemaphore_ContextCancellation` | Client disconnection triggers early return (no leaked goroutines) |

### Integration Tests (require running Docker infrastructure)
| Test | What it proves |
|---|---|
| `TestInventoryConstraints` | Restock works; PostgreSQL `CHECK` constraint rejects negative stock (SQLSTATE 23514) |
| `TestConcurrentReservationZeroOversell` | **1,000 goroutines** race for **100 units** → exactly 100 succeed, 900 rejected, `available_qty = 0` |
| `TestOrderLifecycle_Confirm` | Reserve → Confirm: stock moves from `reserved` to committed, order status = `CONFIRMED` |
| `TestOrderLifecycle_Cancel` | Reserve → Cancel: stock fully restored, order status = `CANCELLED` |
| `TestOrderLifecycle_StateTransitions` | Confirming a `CONFIRMED` order fails; cancelling a `CONFIRMED` order fails (state machine enforcement) |

### Outbox & Relay Tests
| Test | What it proves |
|---|---|
| `TestOutboxRelay_PublishPendingEvents` | Confirm → outbox event → relay publishes to mock Kafka → event marked `PUBLISHED` |
| `TestOutboxRelay_RetryOnFailure` | Kafka failure → `attempts` incremented, event stays `PENDING` for retry |
| `TestOutboxRelay_ReclaimStaleLeases` | Stale `PROCESSING` rows (locked >30s) are reclaimed back to `PENDING` |

### Reaper & SLA Tests
| Test | What it proves |
|---|---|
| `TestReaper_ReclaimExpiredOrder` | Expired reservation → stock restored, order = `EXPIRED` |
| `TestReaper_SkipActiveReservations` | Active (non-expired) reservations are left untouched |
| `TestReaper_SLA_Within5Seconds` | Live reaper daemon reclaims expired stock within 3 seconds (SLA < 5s) |

### Benchmark Tests
| Benchmark | What it measures |
|---|---|
| `TestStrategyComparison` | All three strategies independently enforce zero-oversell: 200 goroutines, 50 units — exactly 50 succeed per strategy |
| `BenchmarkStrategies` | Parallel throughput (ops/sec) for each strategy under sustained load using `b.RunParallel` |

---

## API Reference

All endpoints are prefixed with `/api/v1`.

### Warehouses
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/warehouses` | Create a warehouse with geo-coordinates |
| `GET` | `/warehouses` | List all active warehouses |

### Products
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/products` | Register a new product (SKU, name, weight) |
| `GET` | `/products` | List all products |
| `GET` | `/products/{sku}` | Get product by SKU |

### Inventory
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/inventory/restock` | Add stock to a warehouse (ACID tx + audit log + cache invalidation) |
| `GET` | `/inventory/{sku}` | Get aggregated stock across all warehouses (`X-Cache: HIT\|MISS`) |

### Orders
| Method | Endpoint | Description | Headers |
|---|---|---|---|
| `POST` | `/orders/reserve` | Reserve inventory across optimal warehouses | `Idempotency-Key` (required) |
| `GET` | `/orders/{id}` | Get order details with line items | |
| `POST` | `/orders/{id}/confirm` | Confirm order → commit stock, publish `OrderFulfilled` event | |
| `POST` | `/orders/{id}/cancel` | Cancel order → rollback stock, publish `OrderCancelled` event | |

### System
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Deep health check (pings both PostgreSQL and Redis) |

---

## Tech Stack

| Component | Technology | Role |
|---|---|---|
| **Language** | Go 1.27 | Application server, worker daemons |
| **Database** | PostgreSQL 16 (Alpine) | Source of truth — inventory, orders, outbox, audit log |
| **Cache** | Redis 7 (Alpine) | Read-through cache, contention rate tracking |
| **Message Broker** | Redpanda (Kafka-compatible) | Event streaming for downstream consumers |
| **Router** | chi v5 | HTTP routing with middleware chain |
| **DB Driver** | pgx v5 + pgxpool | High-performance PostgreSQL driver with connection pooling |
| **Code Gen** | sqlc | Type-safe Go from raw SQL queries — no ORM |
| **Containerization** | Docker Compose | Local infrastructure orchestration |

---

## Project Structure

```
Ware_Go/
├── cmd/
│   └── server/
│       └── main.go                          # Entrypoint: pool setup, worker launch, HTTP server, graceful shutdown
├── internal/
│   ├── api/
│   │   ├── router.go                        # Route registration and dependency wiring
│   │   ├── inventory.go                     # Restock (ACID tx) + GetStock (cache-aside)
│   │   ├── orders.go                        # Reserve / Confirm / Cancel / Get
│   │   ├── products.go                      # CRUD for product catalog
│   │   ├── warehouses.go                    # CRUD for warehouses
│   │   ├── response.go                      # Unified JSON response helpers + MaxBytesReader
│   │   └── middleware/
│   │       ├── idempotency.go               # SHA-256 keyed idempotency with response replay
│   │       ├── semaphore.go                 # Channel-based concurrency limiter + load shedding
│   │       └── semaphore_test.go            # Unit tests: pass-through, queue-wait, timeout, ctx cancel
│   ├── config/
│   │   └── config.go                        # Env-based configuration (PORT, DATABASE_URL, REDIS, KAFKA)
│   ├── repository/
│   │   ├── queries/                         # Raw SQL (sqlc input)
│   │   │   ├── inventory.sql                # Upsert restock, reserve (3 strategies), confirm, rollback
│   │   │   ├── orders.sql                   # Order CRUD, warehouse routing query, expired order scan
│   │   │   ├── outbox.sql                   # Insert, fetch SKIP LOCKED, mark published, reclaim stale
│   │   │   ├── idempotency.sql              # Get/Create/Complete idempotency keys
│   │   │   ├── products.sql                 # Product CRUD
│   │   │   └── warehouses.sql               # Warehouse CRUD
│   │   └── db/                              # sqlc-generated Go code (type-safe queries)
│   ├── service/
│   │   ├── reservation_service.go           # Order lifecycle: Reserve (routing + strategy + audit), Confirm, Cancel
│   │   └── routing_service.go               # Haversine-based warehouse ranking with cost function
│   ├── strategy/
│   │   ├── strategy.go                      # ReservationStrategy interface
│   │   ├── pessimistic.go                   # SELECT FOR UPDATE → check → UPDATE
│   │   ├── optimistic.go                    # Read → CAS UPDATE WHERE version=? + backoff/jitter
│   │   ├── atomic.go                        # Single UPDATE WHERE available_qty >= ?
│   │   └── adaptive.go                      # Runtime strategy selector (Redis contention tracking)
│   └── worker/
│       ├── outbox_relay.go                  # Poll outbox → publish to Kafka → mark published
│       ├── reaper.go                        # Reclaim expired reservations → restore stock
│       └── consumer.go                      # Kafka consumer (3PL logistics dispatcher + dedup)
├── migrations/
│   ├── 001_init.sql                         # Core schema: warehouses, products, inventory, orders, audit log
│   └── 002_outbox.sql                       # Outbox table with partial index on PENDING/PROCESSING
├── pkg/
│   └── geo/
│       ├── haversine.go                     # Great-circle distance calculation
│       └── haversine_test.go                # Unit tests with tolerance-based assertions
├── test/
│   └── integration/
│       ├── inventory_test.go                # CHECK constraint validation, restock correctness
│       ├── reservation_test.go              # 1,000-goroutine zero-oversell proof
│       ├── order_lifecycle_test.go           # Full Reserve→Confirm, Reserve→Cancel, state machine tests
│       ├── outbox_test.go                   # Outbox relay publish, retry, stale lease reclaim (mock Kafka)
│       ├── reaper_test.go                   # Reaper reclaim, skip-active, SLA < 5s verification
│       └── benchmark_test.go               # Strategy comparison + parallel benchmarks (b.RunParallel)
├── docker-compose.yml                       # PostgreSQL 16 + Redis 7 + Redpanda (Kafka)
├── sqlc.yml                                 # sqlc configuration (pgx/v5, JSON tags, interface emission)
└── go.mod                                   # Module definition and dependencies
```

---

## Getting Started

**Prerequisites:** Go 1.27+, Docker Desktop

```bash
git clone https://github.com/yashyadav-4/ware_go.git
cd ware_go

# Start infrastructure
docker compose up -d

# Apply schema
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/001_init.sql
docker exec -i Ware_Go_postgres psql -U postgres -d ware_go < migrations/002_outbox.sql

# Run the server
go run cmd/server/main.go
```

```bash
# Health check
curl http://localhost:8080/healthz

# Create a warehouse
curl -X POST http://localhost:8080/api/v1/warehouses \
  -H "Content-Type: application/json" \
  -d '{"code":"BLR-1","name":"Bangalore Hub","latitude":12.9716,"longitude":77.5946}'

# Register a product
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{"sku":"MACBOOK-M3-16","name":"MacBook Pro M3 16-inch","unit_weight_grams":2140}'

# Restock (ACID tx + audit log)
curl -X POST http://localhost:8080/api/v1/inventory/restock \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"<id>","sku":"MACBOOK-M3-16","quantity":50}'

# Read stock — observe X-Cache: MISS then X-Cache: HIT
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16
curl -i http://localhost:8080/api/v1/inventory/MACBOOK-M3-16

# Reserve an order (idempotent)
curl -X POST http://localhost:8080/api/v1/orders/reserve \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-001" \
  -d '{"dest_latitude":28.6139,"dest_longitude":77.2090,"items":[{"sku":"MACBOOK-M3-16","quantity":2}]}'

# Confirm the order
curl -X POST http://localhost:8080/api/v1/orders/<order_id>/confirm
```

---

## Running Tests

```bash
# Unit tests (no infrastructure required)
go test -v ./pkg/geo/...
go test -v ./internal/api/middleware/...

# Integration tests (Docker containers must be running)
go test -v ./test/integration/...

# Race detector — run before every commit
go test -race ./...

# Benchmarks
go test -bench=. -benchmem ./test/integration/...
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server port |
| `DATABASE_URL` | `postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable` | PostgreSQL connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `KAFKA_BROKERS` | `localhost:19092` | Kafka/Redpanda broker address |

---

*Built by [Yash Yadav](https://github.com/yashyadav-4) · Go · PostgreSQL · Redis · Kafka · 2026*
