# Ware_Go — Deep Architecture & Systems Engineering Q&A

A comprehensive reference of all technical, architectural, concurrency, and Go-internals questions discussed during the design and development of **Ware_Go**. 

---

## Table of Contents

### Part 1: High-Level Architecture & Systems Design
1. [Can't we use Kafka for reservations too? Why not store orders in Kafka and batch-process them?](#1-cant-we-use-kafka-for-reservations-too-why-not-store-orders-in-kafka-and-batch-process-them)
2. [If PostgreSQL row-locking serializes writers, how do Amazon and Flipkart handle 50,000 orders per second?](#2-if-postgresql-row-locking-serializes-writers-how-do-amazon-and-flipkart-handle-50000-orders-per-second)
3. [Redis is in-memory and handles 100k+ RPS. Why isn't Redis our primary reservation authority?](#3-redis-is-in-memory-and-handles-100k-rps-why-isnt-redis-our-primary-reservation-authority)
4. [Why do we have 3 locking strategies (Pessimistic, Optimistic, Atomic)? Isn't one enough?](#4-why-do-we-have-3-locking-strategies-pessimistic-optimistic-atomic-isnt-one-enough)
5. [Under high crowd, does Pessimistic hold up? Are we giving up when the crowd is huge?](#5-under-high-crowd-does-pessimistic-hold-up-are-we-giving-up-when-the-crowd-is-huge)
6. [Why not use Redis Redlock for distributed locking?](#6-why-not-use-redis-redlock-for-distributed-locking)
7. [Why not use Sharded Inventory Counters (Slotting)?](#7-why-not-use-sharded-inventory-counters-slotting)
8. [Why not use Queue-Based Event Sourcing for checkout?](#8-why-not-use-queue-based-event-sourcing-for-checkout)
9. [How does `AdaptiveStrategy` dynamically dispatch between strategies at runtime?](#9-how-does-adaptivestrategy-dynamically-dispatch-between-strategies-at-runtime)
10. [Are we going to use Kafka for inventory deductions / returns or not?](#10-are-we-going-to-use-kafka-for-inventory-deductions--returns-or-not)
11. [How many transactions can PostgreSQL handle for `ConfirmOrder`? Why is `ConfirmOrder` much lower contention than `ReserveOrder`?](#11-how-many-transactions-can-postgresql-handle-for-confirmorder-why-is-confirmorder-much-lower-contention-than-reserveorder)
12. [The Dual-Write Problem: Why can't we call Kafka directly before or after `ConfirmOrder`?](#12-the-dual-write-problem-why-cant-we-call-kafka-directly-before-or-after-confirmorder)
13. [Can we use Kafka to batch `ConfirmOrder` and `CancelOrder`? Why keep stock state changes synchronous?](#13-can-we-use-kafka-to-batch-confirmorder-and-cancelorder-why-keep-stock-state-changes-synchronous)

### Part 2: Concurrency, Database Locks & Networking
14. [What is the Global Lock Hierarchy, and how does it mathematically eliminate deadlocks?](#14-what-is-the-global-lock-hierarchy-and-how-does-it-mathematically-eliminate-deadlocks)
15. [Why do Optimistic CAS retries need random jitter instead of fixed delays?](#15-why-do-optimistic-cas-retries-need-random-jitter-instead-of-fixed-delays)
16. [Why does Optimistic CAS with random jitter still collide when traffic reaches 20+ users?](#16-why-does-optimistic-cas-with-random-jitter-still-collide-when-traffic-reaches-20-users)
17. [In `recordAndCheckContention`, why aren't 10,000 Redis increments a bottleneck?](#17-in-recordandcheckcontention-why-arent-10000-redis-increments-a-bottleneck)
18. [What is the Starting Gate pattern (`make(chan struct{})`, `<-startGate`, `close(startGate)`)?](#18-what-is-the-starting-gate-pattern-makechan-struct--startgate-closestartgate)
19. [What is `sync.WaitGroup` (`Add`, `Done`, `Wait`), and how does it differ from JS `Promise.all`?](#19-what-is-syncwaitgroup-add-done-wait-and-how-does-it-differ-from-js-promiseall)
20. [What is `atomic.AddInt64`? Why can't we just write `count++`?](#20-what-is-atomicaddint64-why-cant-we-just-write-count)
21. [What is `go test -race` (ThreadSanitizer), and why did the test take longer with it?](#21-what-is-go-test--race-threadsanitizer-and-why-did-the-test-take-longer-with-it)
22. [Rate Limiting vs. Concurrency Limiting (Admission Control / Semaphore): Why does 100 slots handle 6,000+ RPS?](#22-rate-limiting-vs-concurrency-limiting-admission-control--semaphore-why-does-100-slots-handle-6000-rps)
23. [Why is instant load shedding (`default:`) too harsh, and how does a Bounded Wait Window (`waitTimeout`) fix it?](#23-why-is-instant-load-shedding-default-too-harsh-and-how-does-a-bounded-wait-window-waittimeout-fix-it)

### Part 3: Go Language Internals & HTTP Middleware
24. [What is `go vet`, and how does it differ from `go build`?](#24-what-is-go-vet-and-how-does-it-differ-from-go-build)
25. [If data travels as raw bytes over the network, why and how do we parse it as JSON in Go?](#25-if-data-travels-as-raw-bytes-over-the-network-why-and-how-do-we-parse-it-as-json-in-go)
26. [Why can `r.Body` only be read once? What does `io.NopCloser(bytes.NewReader(bodyBytes))` do?](#26-why-can-rbody-only-be-read-once-what-does-ionopcloserbytesnewreaderbodybytes-do)
27. [How does middleware run code AFTER the main handler in Go? (Go vs Express execution model)](#27-how-does-middleware-run-code-after-the-main-handler-in-go-go-vs-express-execution-model)
28. [In `responseRecorder.WriteHeader`, isn't calling `rec.ResponseWriter.WriteHeader` recursion?](#28-in-responserecorderwriteheader-isnt-calling-recresponsewriterwriteheader-recursion)
29. [Why use `bytes.Buffer` in the response recorder?](#29-why-use-bytesbuffer-in-the-response-recorder)
30. [What happens if a client disconnects or turns off their PC mid-request? Why use `context.Background()`?](#30-what-happens-if-a-client-disconnects-or-turns-off-their-pc-mid-request-why-use-contextbackground)
31. [What does the slice operator `hash[:]` mean? (Array vs Slice)](#31-what-does-the-slice-operator-hash-mean-array-vs-slice)
32. [What is `.git/index.lock`, why does it get stuck, and why is deleting it safe?](#32-what-is-gitindexlock-why-does-it-get-stuck-and-why-is-deleting-it-safe)

### Part 4: Asynchronous Workers, Event Streaming, Go Concurrency & Testing
33. [Why does Redpanda need two ports (9644 vs 19092)?](#33-why-does-redpanda-need-two-ports-9644-vs-19092)
34. [The Kafka Advertised Listener Problem: Why are `kafka-addr` and `advertise-kafka-addr` different?](#34-the-kafka-advertised-listener-problem-why-are-kafka-addr-and-advertise-kafka-addr-different)
35. [Why can't Kafka just be a simple client-server proxy like PostgreSQL? (Direct Broker Routing vs Bottleneck Proxy)](#35-why-cant-kafka-just-be-a-simple-client-server-proxy-like-postgresql-direct-broker-routing-vs-bottleneck-proxy)
36. [How does the Transactional Outbox Pattern work in `002_outbox.sql`, and why use a partial index (`WHERE status IN ('PENDING', 'PROCESSING')`)?](#36-how-does-the-transactional-outbox-pattern-work-in-002_outboxsql-and-why-use-a-partial-index-where-status-in-pending-processing)
37. [What is the difference between `*testing.T` and `*testing.B`? What does `BenchmarkStrategies` actually test?](#37-what-is-the-difference-between-testingt-and-testingb-what-does-benchmarkstrategies-actually-test)
38. [What does `<-blockChan` mean? What is the difference between an unbuffered channel and a buffered channel?](#38-what-does--blockchan-mean-what-is-the-difference-between-an-unbuffered-channel-and-a-buffered-channel)
39. [What does `qtx := r.queries.WithTx(tx)` do?](#39-what-does-qtx--rquerieswithtxtx-do)
40. [Why limit the Reaper to 100 orders per batch instead of reclaiming all expired orders at once?](#40-why-limit-the-reaper-to-100-orders-per-batch-instead-of-reclaiming-all-expired-orders-at-once)
41. [What if 10,000 orders expire at once during a flash sale? Shouldn't the Reaper be Adaptive instead of fixed at 100?](#41-what-if-10000-orders-expire-at-once-during-a-flash-sale-shouldnt-the-reaper-be-adaptive-instead-of-fixed-at-100)
42. [How does `for { select { ... } }` work in Go? Is it parallel or sequential, and how does Go decide between ready channels?](#42-how-does-for--select----work-in-go-is-it-parallel-or-sequential-and-how-does-go-decide-between-ready-channels)
43. [What does `slog.Default().With("component", "reaper")` mean, and why use structured logging tags in production?](#43-what-does-slogdefaultwithcomponent-reaper-mean-and-why-use-structured-logging-tags-in-production)
44. [What are the core components of a Kafka message (`Topic`, `Key`, `Value`, `Headers`, `Timestamp`), and what role does each play in Ware_Go?](#44-what-are-the-core-components-of-a-kafka-message-topic-key-value-headers-timestamp-and-what-role-does-each-play-in-ware_go)

### Part 5: Load Testing, Observability & Performance Profiling (Week 6)
45. [Why use k6 instead of Postman or JMeter for concurrency and load testing?](#45-why-use-k6-instead-of-postman-or-jmeter-for-concurrency-and-load-testing)
46. [Why did our k6 script fail initially with a database error on warehouse code `VARCHAR(20)`?](#46-why-did-our-k6-script-fail-initially-with-a-database-error-on-warehouse-code-varchar20)
47. [Why did `s1_zero_oversell.js` require unique Idempotency Keys across VUs?](#47-why-did-s1_zero_overselljs-require-unique-idempotency-keys-across-vus)
48. [How does the Reaper SLA test (`s2_zombie_stock.js`) work with fast TTLs?](#48-how-does-the-reaper-sla-test-s2_zombie_stockjs-work-with-fast-ttls)
49. [How does Scenario 3 (`s3_deadlock.js`) prove deadlock freedom under opposing cross-warehouse orders?](#49-how-does-scenario-3-s3_deadlockjs-prove-deadlock-freedom-under-opposing-cross-warehouse-orders)
50. [How does Scenario 4 (`s4_latency.js`) measure latency curves under sustained load?](#50-how-does-scenario-4-s4_latencyjs-measure-latency-curves-under-sustained-load)
51. [How does the Semaphore Load Shedder prevent Cascading Failures during traffic surges?](#51-how-does-the-semaphore-load-shedder-prevent-cascading-failures-during-traffic-surges)
52. [Prometheus & Grafana Architecture: How does metric collection work in Ware_Go without slowing down requests?](#52-prometheus--grafana-architecture-how-does-metric-collection-work-in-ware_go-without-slowing-down-requests)
53. [How does Go `pprof` profiling work, and how do we capture CPU, heap, and goroutine profiles?](#53-how-does-go-pprof-profiling-work-and-how-do-we-capture-cpu-heap-and-goroutine-profiles)

### Part 6: AI Inventory Rebalancer, Quota Optimization & Production Architecture (Week 7)
54. [What is the AI Inventory Rebalancing Agent, and why not use pure rule-based logic?](#54-what-is-the-ai-inventory-rebalancing-agent-and-why-not-use-pure-rule-based-logic)
55. [Model Lifecycle & Free Tier Quota Economics: Why did we switch from Gemini 1.5/2.0 to 3.8 and finally to 3.5 Flash Lite?](#55-model-lifecycle--free-tier-quota-economics-why-did-we-switch-from-gemini-1520-to-38-and-finally-to-35-flash-lite)
56. [Dynamic LLM Configuration: Why make the model name configurable in `.env`?](#56-dynamic-llm-configuration-why-make-the-model-name-configurable-in-env)
57. [Why did `/api/v1/agent/proposals` fail initially with `curl (52) Empty reply from server`?](#57-why-did-apiv1agentproposals-fail-initially-with-curl-52-empty-reply-from-server)
58. [Stateful Proposal Execution & Two-Phase Safety: Why store AI proposals in PostgreSQL before execution?](#58-stateful-proposal-execution--two-phase-safety-why-store-ai-proposals-in-postgresql-before-execution)
59. [Multi-Stage Dockerfile: Why do we use a multi-stage build, and how does it shrink image size from 1+ GB to 18.1 MB?](#59-multi-stage-dockerfile-why-do-we-use-a-multi-stage-build-and-how-does-it-shrink-image-size-from-1-gb-to-181-mb)
60. [Unprivileged User Security in Docker: Why create `appuser` and `appgroup`?](#60-unprivileged-user-security-in-docker-why-create-appuser-and-appgroup)
61. [Why did `RUN go mod download` time out in Docker, and why is `go mod vendor` the production solution?](#61-why-did-run-go-mod-download-time-out-in-docker-and-why-is-go-mod-vendor-the-production-solution)
62. [Docker Images: What is the difference between `DISK USAGE` and `CONTENT SIZE`?](#62-docker-images-what-is-the-difference-between-disk-usage-and-content-size)
63. [Horizontal Scaling vs Database Bottleneck: Does adding more containers help when 10,000 requests hit PostgreSQL?](#63-horizontal-scaling-vs-database-bottleneck-does-adding-more-containers-help-when-10000-requests-hit-postgresql)
64. [Single-VM Architecture: Can we run Nginx, App, Postgres, Redis, and Kafka all on one virtual machine? How does traffic distribution work?](#64-single-vm-architecture-can-we-run-nginx-app-postgres-redis-and-kafka-all-on-one-virtual-machine-how-does-traffic-distribution-work)
65. [Managed Cloud Services vs Self-Hosted in `docker-compose.yml`: How do external Database URLs work?](#65-managed-cloud-services-vs-self-hosted-in-docker-composeyml-how-do-external-database-urls-work)
66. [Docker Networks: What does `networks: ware_go_net` and `driver: bridge` mean?](#66-docker-networks-what-does-networks-ware_go_net-and-driver-bridge-mean)

---

# Part 1: High-Level Architecture & Systems Design

### 1. Can't we use Kafka for reservations too? Why not store orders in Kafka and batch-process them?

**Yes, you can, but it fundamentally alters the user experience and multi-item carts.**

In a Kafka-first queue model:
1. The user clicks "Buy Now".
2. The HTTP API publishes an `OrderRequested` event to Kafka and immediately responds with `202 Accepted` ("You are in line...").
3. Background workers consume from Kafka and deduct stock sequentially.
4. The user's frontend polls an endpoint or waits on a WebSocket for 2 to 5 seconds.

**Why this breaks down for Multi-Warehouse / Multi-Item Carts:**
- **The Partitioning Dilemma**: In Kafka, strict FIFO order with zero locks only works if you partition by `SKU` (all orders for a MacBook go to 1 consumer thread).
- If a customer buys a **MacBook + Mouse + Keyboard** in one cart:
  - The MacBook goes to Partition 0.
  - The Mouse goes to Partition 1.
  - The Keyboard goes to Partition 2.
- Now three independent workers process parts of the same cart. If the keyboard is out of stock, you must execute a distributed **Saga compensation pattern** across topics to rollback the reserved laptop and mouse!
- If you partition by `user_id` instead to keep the cart together, different workers will process orders for the same SKU simultaneously — **putting you right back to needing database row locks!**

**Ware_Go Decision**: Use synchronous relational ACID for instant customer confirmation (15 ms latency), and use Kafka **downstream** via the **Transactional Outbox Pattern** for fulfillment, billing, and shipping.

---

### 2. If PostgreSQL row-locking serializes writers, how do Amazon and Flipkart handle 50,000 orders per second?

A single database row lock serializes operations on that **one specific row**. If a row lock takes 2 ms:
$$\frac{1,000\text{ ms}}{2\text{ ms}} \approx 500\text{ reservations/sec for that SINGLE row.}$$

Hyperscalers handle 50,000+ RPS through four architectural layers:

1. **Broad Catalog Distribution**: If 50,000 users checkout simultaneously, User A buys diapers in Bangalore, User B buys a book in Mumbai, and User C buys shoes in Delhi. They touch completely different database rows and run in parallel across CPU cores without blocking each other.
2. **Spatial / Warehouse Sharding**: Stock in Ware_Go is keyed by `(warehouse_id, sku)`. A MacBook in Bangalore (`BLR-1`) is a completely different row from a MacBook in Delhi (`DEL-1`). Traffic is geographically partitioned.
3. **Redis Gatekeeper (Token Bucket)**: For extreme flash sales (50,000 users for 500 units), requests hit Redis first. A Redis Lua script decrements an in-memory counter in 0.1 ms. Only the 500 winners get a token to write to PostgreSQL. The 49,500 losers are rejected at the Redis layer in 0.5 ms without ever touching the database.
4. **Single-Statement Atomic Updates**: Replacing two-step `SELECT ... FOR UPDATE` with `UPDATE ... WHERE available_qty >= qty` shrinks lock duration from milliseconds (across network round-trips) to **microseconds** inside the database engine.

---

### 3. Redis is in-memory and handles 100k+ RPS. Why isn't Redis our primary reservation authority?

There are three fatal drawbacks to making Redis the primary reservation authority for multi-warehouse inventory:

1. **Geographic Routing Math in RAM**: Reserving stock requires Haversine distance calculations from customer coordinates, finding all active warehouses stocking the SKU, computing stock ratios, and splitting quantities. PostgreSQL does relational joins and math natively. Doing this in Redis requires complex, long-running Lua scripts that block Redis's single thread.
2. **The Distributed Dual-Write Bug (Phantom Stock Loss)**:
   - Redis decrements stock: `10 → 9` (Success).
   - PostgreSQL creates order: `INSERT INTO orders...` (FAILS due to connection drop, timeout, or constraint).
   - Now Redis says stock is 9, but Postgres has no order. 1 unit vanished into thin air. Compensating distributed failures between Redis and Postgres is notoriously bug-prone.
3. **The Audit Ledger Invariant**: Ware_Go guarantees that every stock change has an immutable accounting record in `inventory_audit_log` in the **exact same ACID transaction**. Redis cannot participate in PostgreSQL ACID transactions.

**The Role Division**: Redis is our **Read Accelerator** (`GET /inventory/:sku` via Cache-Aside at 0.3 ms). PostgreSQL is our **Source of Truth Authority**.

---

### 4. Why do we have 3 locking strategies (Pessimistic, Optimistic, Atomic)? Isn't one enough?

No single locking strategy is universally superior. Each excels under a specific workload and collapses under another:

| Strategy | Best Workload | Mechanism | Failure Mode Under Wrong Workload |
|---|---|---|---|
| **Pessimistic (`FOR UPDATE`)** | **High-Contention Flash Sales** (10,000 users, 100 units) | Exclusive row lock queues writers | Slower for wide catalogs with low contention (unnecessary lock wait) |
| **Optimistic (CAS)** | **Low-to-Medium Contention** (General e-commerce browsing) | Lock-free; `WHERE version = $old` | Collapses into **Retry Storms** under flash sales |
| **Atomic Conditional** | **Single-Item High-Throughput** | 1-step `WHERE available_qty >= $N` | Cannot coordinate multi-warehouse split orders in one statement |

Having all three behind a clean `ReservationStrategy` interface lets us benchmark them under identical load and prove with data when each should be used.

---

### 5. Under high crowd, does Pessimistic hold up? Are we giving up when the crowd is huge?

**No, we are not giving up.** 

Under extreme contention (10,000 requests for 100 units):
- **Optimistic CAS collapses into a Death Spiral**: 10,000 requests all collide. 1 wins, 9,999 fail and retry. The retries bombard the database with 30,000+ queries, CPU hits 100%, and p99 latency explodes past 2,000 ms.
- **Pessimistic acts like a Turnstile**: `SELECT ... FOR UPDATE` lines requests up in an orderly queue. The first 100 acquire the lock, decrement stock, and commit. Transaction #101 acquires the lock, sees `available_qty == 0`, and immediately rejects with Out of Stock. **There are zero wasted retry queries.** PostgreSQL processes ~1,000 RPS predictably with stable latency.

Furthermore, over 70% of flash-sale checkouts are for a **single item**, where the **Atomic Conditional Update** runs in microseconds directly in database memory at 5,000+ RPS.

---

### 6. Why not use Redis Redlock for distributed locking?

Martin Kleppmann (author of *Designing Data-Intensive Applications*) mathematically proved that **Redis Redlock is fundamentally unsafe for strict consistency**:

1. **Process Pauses (GC pauses)**: Process A acquires the Redlock with a 10s TTL. Process A hits a 12s garbage collection pause. The lock expires. Process B acquires the lock. Process A wakes up and writes, believing it still has the lock. Both write simultaneously $\rightarrow$ **Oversell**.
2. **Clock Drift**: Redlock relies on physical system clocks across multiple Redis nodes. System clocks jump forward due to NTP updates, prematurely expiring locks.
3. **Network Delays**: Split-brain scenarios between Redis nodes allow multiple processes to acquire quorum on the same key.

**Verdict**: Redis locks are fine for non-critical tasks (e.g. preventing double-rendering an email). For financial transactions and inventory where overselling costs money, **relational storage engine locks (`FOR UPDATE` with WAL and engine-level `CHECK` constraints)** are non-negotiable.

---

### 7. Why not use Sharded Inventory Counters (Slotting)?

Sharded counters split 10,000 units of stock across 10 internal database rows (1,000 units in Slot 1, 1,000 in Slot 2, etc.) to multiply write throughput by 10x.

**Why we didn't use it in Week 4**:
- **Inventory Fragmentation**: What if Customer A orders 5 units, but Slot 1 has 3 and Slot 2 has 2? Your application must coordinate distributed cross-slot borrowing, which re-introduces cross-row locks, complex rollbacks, and deadlocks.
- **Premature Complexity**: Sharded counters are only justified when a single SKU exceeds 10,000 writes/sec. Our spatial warehouse sharding (`warehouse_id, sku`) already partitions stock naturally across physical locations.

---

### 8. Why not use Queue-Based Event Sourcing for checkout?

In pure Event Sourcing, the database only stores an append-only log of events (`OrderRequested`, `StockDeducted`), and current stock is calculated by replaying events or updating materialized views asynchronously.

**The Trade-off**:
- Event Sourcing is **asynchronous** and **eventually consistent**. The client gets `202 Accepted` and must poll for a confirmation.
- Ware_Go's explicit design requirement is **synchronous ACID confirmation**: the customer receives a definitive `201 Created` with guaranteed stock reservation in **under 25 ms**.
- We use event streaming where it belongs: **downstream** via the Transactional Outbox Pattern for warehouse packing, carrier assignment, and notifications.

---

### 9. How does `AdaptiveStrategy` dynamically dispatch between strategies at runtime?

The `AdaptiveStrategy` acts as an intelligent runtime router for reservations:

1. **Rule 1 (Cart Shape)**:
   - If `len(items) == 1`: Routes directly to **`AtomicStrategy`**. A single-item cart runs in **microseconds** via single-statement conditional update without touching Redis or holding multi-statement locks.
2. **Rule 2 (Contention Heuristic)**:
   - If multi-item, checks the 1-second rolling velocity counter in Redis (`rate:sku:<sku>`):
     - **Low Contention (< 10 requests/sec)**: Dispatches to **`OptimisticStrategy`** (lock-free CAS with exponential backoff and random jitter).
     - **High Contention / Flash Sale ($\ge$ 10 requests/sec)**: Dispatches to **`PessimisticStrategy`** (`SELECT ... FOR UPDATE`) to queue buyers cleanly and prevent retry storms.
3. **Rule 3 (Benchmark Override)**:
   - If a caller passes `X-Reservation-Strategy: pessimistic | optimistic | atomic` via context, it honors that exact strategy so our benchmark suite can test each in isolation!

---

### 10. Are we going to use Kafka for inventory deductions / returns or not?

**No, we are NOT using Kafka for inventory deductions or cancellations.**

1. **Immediate Stock Truth**: If Customer A cancels an order, or if an order expires, the stock must return to `available_qty` **immediately in PostgreSQL** so another waiting buyer can see it and purchase it. If inventory returns were delayed in a Kafka batch queue, other buyers would see "Out of Stock" for 2–5 seconds even when stock is physically available.
2. **Instant User Feedback**: When a customer confirms or cancels, they need a guaranteed `200 OK` right away.
3. **Where Kafka IS Used**: In **Week 5**, Kafka (via Redpanda) is used for **Downstream Fulfillment via the Transactional Outbox Pattern**. Once PostgreSQL commits the order confirmation, the outbox relay pushes the event to Kafka for warehouse packaging, shipping label generation, customer emails, and analytics.

---

### 11. How many transactions can PostgreSQL handle for `ConfirmOrder`? Why is `ConfirmOrder` much lower contention than `ReserveOrder`?

In a flash sale, 10,000 users click "Buy" at the **exact same millisecond** (12:00:00.000). They all fight for the **exact same database row** (`available_qty`).

`ConfirmOrder` has almost **zero contention** because:
1. **Traffic is smeared across time**: User A confirms at 12:01:14 (entering credit card). User B confirms at 12:00:42 (UPI OTP). User C confirms at 12:02:05. Instead of 10,000 requests hitting in 1 millisecond, the 10,000 confirmations are **naturally smeared across a 5-minute window**.
2. **Stock is ALREADY secured**: `ConfirmOrder` does not fight for `available_qty`. It only updates `reserved_qty = reserved_qty - qty`. Nobody can take their item away because it was already locked during reservation.

A standard PostgreSQL 16 instance on modern SSDs handles **2,500 to 5,000 committed write transactions per second**. Because `ConfirmOrder` touches individual, non-colliding order rows, PostgreSQL easily handles **over 3,000 confirmations per second**.

---

### 12. The Dual-Write Problem: Why can't we call Kafka directly before or after `ConfirmOrder`?

If you try to call Kafka directly inside the checkout HTTP handler, both approaches have fatal failure modes:

- **Danger A (Calling Kafka BEFORE `tx.Commit()`)**:
  Go sends `ORDER_CONFIRMED` to Kafka. The warehouse shipping team packs and ships the laptop. THEN, `tx.Commit()` fails in PostgreSQL (disk full, network timeout). The order is rolled back in the DB, but the item was already shipped. **You just gave away a free \$2,000 laptop.**
- **Danger B (Calling Kafka AFTER `tx.Commit()`)**:
  `tx.Commit()` succeeds. Then Go tries to send to Kafka, but Kafka is temporarily down, or the server crashes. The customer was charged, PostgreSQL says `CONFIRMED`, but Kafka never got the event. **The order sits in a database black hole forever and is never shipped.**

**The Solution: The Transactional Outbox Pattern (Week 5)**:
We insert an outbox event into an `outbox` table **inside the exact same ACID transaction** as `ConfirmOrder`. If the transaction rolls back, the event rolls back. If the transaction commits, the event is guaranteed to be saved on disk. A background relay worker drains the outbox to Kafka safely.

---

### 13. Can we use Kafka to batch `ConfirmOrder` and `CancelOrder`? Why keep stock state changes synchronous?

Batching 50 confirmations into a single SQL update reduces database disk I/O by 90%. However, keeping state changes synchronous in PostgreSQL is necessary due to two critical pitfalls:

1. **Cancellation Latency**: If Customer A cancels an order, and the return is queued in a 5-second Kafka batch, waiting buyers see "Out of Stock" for 5 seconds even though the item was cancelled. Synchronous cancellation returns stock **instantly**.
2. **Payment Gateway Webhook Sync**: When a payment gateway webhooks `POST /webhook/payment-succeeded`, the user's browser immediately polls `GET /orders/123`. If confirmation is buffered in Kafka, the user sees `RESERVED` instead of `CONFIRMED` and panics thinking their payment was lost.

Therefore, state changes happen immediately in PostgreSQL, while **downstream heavy work** (shipping label generation, emails, invoice PDFs) is batched in Kafka!

---

# Part 2: Concurrency, Database Locks & Networking

### 14. What is the Global Lock Hierarchy, and how does it mathematically eliminate deadlocks?

**Why deadlocks happen**:
- Transaction 1 locks `[SKU-A, SKU-B]`. It locks SKU-A, then waits for SKU-B.
- Transaction 2 locks `[SKU-B, SKU-A]`. It locks SKU-B, then waits for SKU-A.
- Both wait on each other forever $\rightarrow$ **Deadlock!** PostgreSQL forcibly kills one with `SQLSTATE 40P01`.

**The Elimination Rule**:
Before touching the database, **every code path (`Reserve`, `Confirm`, `Cancel`, and the `Reaper`) must sort items in the exact same global order**:
1. `warehouse_id` ASC (byte comparison)
2. `sku` ASC (alphabetical comparison)

Because Transaction 1 and Transaction 2 both sort their items first, both will attempt to lock SKU-A first, then SKU-B. Transaction 2 simply queues behind Transaction 1. A cycle in the lock wait graph is **mathematically impossible**.

---

### 15. Why do Optimistic CAS retries need random jitter instead of fixed delays?

If 50 requests collide at the same millisecond on version 1, one succeeds and 49 fail.

**If you use a fixed delay (`time.Sleep(5 * time.Millisecond)`):**
- All 49 requests sleep for exactly 5 ms.
- All 49 wake up at the exact same nanosecond and attack the database together.
- 1 succeeds, 48 fail, sleep 10 ms, wake up together, and collide again!
- This is the **Thundering Herd / Lockstep Resonance Problem**.

**With Random Jitter:**
$$\text{sleep} = \text{base} \times 2^{\text{attempt}} + \text{rand}(0, 10\text{ms})$$
- Request 1 sleeps for 6 ms.
- Request 2 sleeps for 12 ms.
- Request 3 sleeps for 8 ms.
- Competing requests are desynchronized across time, allowing each to find an open window and succeed on retry.

---

### 16. Why does Optimistic CAS with random jitter still collide when traffic reaches 20+ users?

An update is **not an instantaneous zero-second event**.  
Between when Go reads `version = 2` and when PostgreSQL finally commits `version = 3`, there is a **4 to 6-millisecond vulnerability window** (network round-trip + WAL disk write).

If 20 users pick random jitter times between 0 ms and 20 ms:
- By the **Birthday Paradox / Poisson Distribution**, the probability of two or more users landing within the same 5 ms vulnerability window is **over 85%**!
- User B reads `version = 2` while User A is in the middle of updating it. User A commits `version = 3`. User B tries to commit with `WHERE version = 2` $\rightarrow$ **Collision!**
- With `maxRetries = 3`, at most 4 users can ever succeed out of 20 simultaneous colliding writers! The other 16 fail their checkouts.

That is why at **$\ge$ 10–20 requests/sec on a single row**, `AdaptiveStrategy` switches to **Pessimistic (`FOR UPDATE`)**, where all 20 queue cleanly and 100% of them succeed in ~40 ms.

---

### 17. In `recordAndCheckContention`, why aren't 10,000 Redis increments a bottleneck?

1. **Redis Hardware Speed**: Redis is single-threaded in-memory C. A single Redis core easily handles **100,000 to 150,000 operations per second** at ~0.1 ms per operation. 10,000 requests/sec uses only ~7% of a single Redis CPU core.
2. **Pipelining**: `s.rdb.Pipeline()` bundles both `INCR` and `EXPIRE` into **one single TCP network packet**.
3. **Single-Item Carts Bypass Redis Completely**: In `AdaptiveStrategy`, Rule 1 checks:
   ```go
   if len(items) == 1 {
       return s.atomic.Reserve(ctx, qtx, items)
   }
   ```
   Over 70% of flash-sale checkouts are single-item. They **never touch Redis at all**! Redis is only contacted for complex multi-item carts.

---

### 18. What is the Starting Gate pattern (`make(chan struct{})`, `<-startGate`, `close(startGate)`)?

If you launch 1,000 goroutines in a loop:
```go
for i := 0; i < 1000; i++ {
    go func() { reserve() }()
}
```
Goroutine #1 will finish before Goroutine #800 is even scheduled by the Go runtime. That is a staggered sequence, not a concurrency collision.

**The Starting Gate**:
1. `startGate := make(chan struct{})`: An unbuffered channel (a closed gate).
2. Inside each goroutine: `<-startGate` tells the goroutine to freeze and wait.
3. Once all 1,000 goroutines are created and waiting at the line, the main test thread calls `close(startGate)`.
4. In Go, **closing a channel broadcasts simultaneously to all listening goroutines**. The gate swings open, and all 1,000 goroutines hit PostgreSQL at the exact same microsecond.

---

### 19. What is `sync.WaitGroup` (`Add`, `Done`, `Wait`), and how does it differ from JS `Promise.all`?

In Node.js, `Promise.all([p1, p2])` pauses until all promises resolve. In Go, goroutines do not return promises. If the main function exits, all running background goroutines are instantly terminated.

Think of `sync.WaitGroup` as a **tour bus counter**:
- **`wg.Add(1000)`**: The tour guide notes that 1,000 tourists left the bus (counter = 1,000).
- **`defer wg.Done()`**: Each tourist decrements the counter by 1 when returning to the bus (`1000 → 999 → 998...`).
- **`wg.Wait()`**: The bus driver blocks at the door until the counter reaches exactly 0 before driving away.

---

### 20. What is `atomic.AddInt64`? Why can't we just write `count++`?

In computer hardware, `count++` is **three separate CPU instructions**:
1. Read `count` from RAM into a CPU register.
2. Increment the register by 1.
3. Write the register back to RAM.

If Goroutine A and Goroutine B run at the same time:
- Both read `5` from RAM.
- Both increment to `6`.
- Both write `6` to RAM.
- Two operations occurred, but the counter only moved by 1! This is a **data race**.

**`atomic.AddInt64`** issues a hardware bus lock instruction (`LOCK XADD` on x86 CPUs) that increments the 64-bit integer directly in memory in a single atomic CPU cycle. It is 100% thread-safe across parallel cores.

---

### 21. What is `go test -race` (ThreadSanitizer), and why did the test take longer with it?

**ThreadSanitizer** is a specialized runtime analysis tool created by Google.

When you pass the `-race` flag:
1. The Go compiler injects monitoring code around **every single memory read and write** in your program.
2. It tracks memory access histories in "shadow memory".
3. If two goroutines access the same variable concurrently without synchronization, ThreadSanitizer crashes the test and prints the exact lines of code where the race occurred.

**Why it was slower (1.6s vs 5.7s)**:
- CPU overhead increases 2x–5x due to the injected checks.
- Memory usage increases 5x–10x to maintain shadow memory.
- Running 1,000 concurrent goroutines under `-race` with **zero warnings** mathematically proves your Go code has zero memory data races.

---

### 22. Rate Limiting vs. Concurrency Limiting (Admission Control / Semaphore): Why does 100 slots handle 6,000+ RPS?

- **Rate Limiting** measures **frequency over time** (e.g. 10 requests per minute per user).
- **Concurrency Limiting (Semaphore)** measures **in-flight active requests right now**, regardless of time.

Because our reservations take only **15 milliseconds**, once a request finishes, it **immediately releases its slot** (`defer func() { <-sem }()`).
$$\frac{1,000\text{ ms (1 second)}}{15\text{ ms per request}} \approx 66\text{ requests per slot per second.}$$
$$66 \times 100\text{ slots} = \mathbf{6,600\text{ requests per second!}}$$

A concurrency limit of 100 easily handles over 6,000 orders/sec while guaranteeing that our 25-connection database pool is never overwhelmed by 5,000 piled-up goroutines.

---

### 23. Why is instant load shedding (`default:`) too harsh, and how does a Bounded Wait Window (`waitTimeout`) fix it?

In a naive semaphore with `default:`, if all 100 slots are busy at 12:00:00.000, request #101 is dropped in 0.001 ms, **even though a slot was going to open up at 12:00:00.003 (just 2 milliseconds later)!**

By adding a **100 ms wait window** (`time.After(100 * time.Millisecond)`):
- In 100 ms, since requests take 15 ms, **hundreds of slots open up**!
- Transient bursts of 200–300 users pause for 5–15 ms, get a slot, and succeed.
- Only sustained, catastrophic overload lasting longer than 100 ms is shed with `503 Service Unavailable`.

---

# Part 3: Go Language Internals & HTTP Middleware

### 24. What is `go vet`, and how does it differ from `go build`?

- **`go build`** is the **compiler**. It checks: *"Is the syntax valid, do types match, and can I generate machine code?"*
- **`go vet`** is the **linter / static analyzer** (like ESLint for JS). It checks for subtle logical bugs that compile fine but blow up at runtime:
  - Format verb mismatches: `fmt.Printf("%d", "hello")`
  - Variable shadowing: accidentally declaring a local `err := ...` that hides the outer error.
  - Passing a `sync.Mutex` by value instead of pointer (copying a lock breaks mutual exclusion).

---

### 25. If data travels as raw bytes over the network, why and how do we parse it as JSON in Go?

**All network cables only carry binary numbers (bytes)**. JSON is just text characters (`{`, `"`, `s`, `k`, `u`), and every character is an integer byte (`{` is byte `123`).

- **In Node.js**: `express.json()` forces every stream into a heavy dynamic JavaScript object with prototype chains and hash-map lookups.
- **In Go (`json.NewDecoder(r.Body).Decode(&payload)`)**: The decoder reads byte numbers from the network stream and writes the values **directly into the fixed memory addresses** of your Go struct `payload`. There is zero prototype overhead and minimal memory allocation.
- **When we don't need fields**: Go lets us work with raw bytes! In `idempotency.go`, we hashed `bodyBytes` with SHA-256 without ever parsing JSON. In `inventory.go`, Redis cache hits write raw bytes directly to the client socket (`w.Write(cachedBytes)`), skipping JSON parsing entirely.

---

### 26. Why can `r.Body` only be read once? What does `io.NopCloser(bytes.NewReader(bodyBytes))` do?

In Go, `r.Body` is an **`io.ReadCloser`** (an open network TCP stream). When you read from a stream, the network card moves bytes from the buffer into your slice. Once read, the stream is **empty**.

If our idempotency middleware reads `r.Body` to compute the SHA-256 hash, the downstream JSON handler (`orders.go`) will read an empty stream and fail.

**How we restore it**:
1. `bodyBytes, _ := io.ReadAll(r.Body)`: Read all bytes into memory.
2. `bytes.NewReader(bodyBytes)`: Creates a new readable stream from the memory slice.
3. `io.NopCloser(...)`: Go's `r.Body` requires two methods: `Read()` AND `Close()`. In-memory bytes don't need closing. `io.NopCloser` wraps the reader and provides a dummy "No-Operation" `Close()` method so it satisfies the `io.ReadCloser` interface.

---

### 27. How does middleware run code AFTER the main handler in Go? (Go vs Express execution model)

In Express.js (Node.js), `next()` is callback-based, making it difficult to inspect the finished response without monkey-patching `res.send`.

In Go, **`next.ServeHTTP(w, r)` is a standard synchronous function call**:
```go
// 1. Code here runs BEFORE the handler (check headers, read body)
next.ServeHTTP(recorder, r)
// 2. Code here runs AFTER the handler has completely finished!
```
When `next.ServeHTTP` returns, the handler has already committed the transaction and written the response! Our middleware can now read the captured HTTP status code (`201 Created`) and response JSON bytes from `recorder`, and save them into the `idempotency_key` table.

---

### 28. In `responseRecorder.WriteHeader`, isn't calling `rec.ResponseWriter.WriteHeader` recursion?

```go
func (rec *responseRecorder) WriteHeader(code int) {
    rec.statusCode = code
    rec.ResponseWriter.WriteHeader(code) // NOT recursion!
}
```

**It is NOT recursion.**

Notice that it does not call `rec.WriteHeader(code)`. It calls `rec.ResponseWriter.WriteHeader(code)`.
- `rec` is our custom wrapper struct.
- `rec.ResponseWriter` is the **original `w`** passed by Go's HTTP server (the actual TCP network socket).

It is delegating to the inner embedded interface (like calling `super.writeHeader(code)` in OOP). We note down the status code in memory, then forward the call to the real network connection.

---

### 29. Why use `bytes.Buffer` in the response recorder?

A standard byte slice `[]byte` has a fixed capacity. A **`bytes.Buffer`** is an in-memory, auto-growing bucket that implements `io.Writer`.

When downstream controllers write JSON in chunks, `bytes.Buffer` dynamically expands to hold them. Once the handler finishes, we call `recorder.body.Bytes()` to extract the complete response JSON in one clean slice to store in PostgreSQL.

---

### 30. What happens if a client disconnects or turns off their PC mid-request? Why use `context.Background()`?

When a client closes their browser or loses internet:
1. The TCP socket dies.
2. Go's HTTP server detects the closed socket and **cancels `r.Context()`**.
3. If the database transaction was still running, PostgreSQL aborts and rolls back. Stock is safe!

**The Disconnect Trap on Completion**:
If the order committed in PostgreSQL, but the client disconnected before receiving the HTTP packet, our middleware tries to run:
```go
queries.CompleteIdempotencyKey(r.Context(), ...)
```
Because `r.Context()` was canceled by the client disconnect, PostgreSQL will reject this query with `context canceled`, leaving the idempotency key permanently stuck in `IN_PROGRESS`!

**The Fix**:
```go
completeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
```
We use a detached `context.Background()` with a 5-second timeout. This guarantees our server has 5 seconds to mark the order as `COMPLETED` in the database. When the user reconnects and retries with the same `Idempotency-Key`, the server returns the completed order without double-charging them!

---

### 31. What does the slice operator `hash[:]` mean? (Array vs Slice)

- `[32]byte` is a **fixed-size array** (exactly 32 bytes allocated inline).
- `[]byte` is a **slice** (a dynamically-sized view into memory).

The function `sha256.Sum256(...)` returns a fixed array `[32]byte`.  
However, `hex.EncodeToString(...)` expects a slice `[]byte`.

The syntax `hash[:]` is the **slice operator**. It creates a slice pointing to the entire underlying 32-byte array from index 0 to the end, with **zero memory copying**.

---

### 32. What is `.git/index.lock`, why does it get stuck, and why is deleting it safe?

When you run any Git staging or commit command (`git add`, `git commit`), Git creates a lock file at `.git/index.lock` to prevent two Git processes from modifying the index simultaneously.

If a command crashes, is killed, or hits a permission error mid-way, Git never gets to clean up `index.lock`. Future Git commands fail with:
`fatal: Unable to create '.git/index.lock': File exists.`

**Is it safe to delete?**  
Yes. As long as no other Git command is running in another terminal, deleting `index.lock` (`del .git\index.lock`) simply clears the stale lock so Git can operate normally. No code or commit history is ever lost.

---

# Part 4: Asynchronous Workers, Event Streaming, Go Concurrency & Testing

### 33. Why does Redpanda need two ports (9644 vs 19092)?

Redpanda separates the **Data Plane** from the **Control Plane**:
1. **Port 19092 (Kafka Wire Protocol)**: High-throughput binary protocol used strictly for publishing and consuming partition records (`OrderReserved`, `OrderFulfilled`).
2. **Port 9644 (Admin HTTP REST API)**: Standard JSON API used by Prometheus scrapers, cluster management CLIs (`rpk cluster health`), and container healthchecks.

Separating them guarantees administrative monitoring probes never compete with or slow down the high-speed binary message streaming path.

---

### 34. The Kafka Advertised Listener Problem: Why are `kafka-addr` and `advertise-kafka-addr` different?

When a client connects to Kafka, it performs a two-step handshake:
1. Client connects to a bootstrap broker.
2. The broker returns **Metadata** telling the client the exact IP/port of the partition leader to send data to.

If Redpanda is running inside Docker:
* Its internal container IP is `172.18.x.x`.
* If it returned that internal IP to a client running on Windows, the client would crash with `Connection Refused` because Windows has no route to Docker's private bridge network.

**The Solution**:
* **`--kafka-addr`**: Tells the broker what socket to bind to inside the container (`0.0.0.0:19092`).
* **`--advertise-kafka-addr`**: Tells the broker what address to advertise back to clients in metadata responses. We advertise `OUTSIDE://localhost:19092` so host machines can connect, and `PLAINTEXT://redpanda:29092` for inter-container communication.

---

### 35. Why can't Kafka just be a simple client-server proxy like PostgreSQL? (Direct Broker Routing vs Bottleneck Proxy)

In standard client-server databases like PostgreSQL, you send a query to port 5432, it executes, and returns.

Kafka cannot do this because it is engineered for **millions of events per second across dozens of servers**:
* If Broker 1 had to act as a proxy (accepting messages for Partition 4, forwarding them to Broker 7, waiting for ACK, and forwarding back), Broker 1 would become a massive bottleneck.
* Instead, Kafka uses **Direct Broker Routing**: you connect to any broker once to get the cluster map, and your client then opens a **direct TCP socket to the specific broker that owns the partition**.
* This eliminates proxy bottlenecks completely and enables linear horizontal scalability.

---

### 36. How does the Transactional Outbox Pattern work in `002_outbox.sql`, and why use a partial index (`WHERE status IN ('PENDING', 'PROCESSING')`)?

1. **Transactional Outbox**:
   Writing to Postgres and then calling Kafka separately creates the **Dual-Write Problem**: if the server crashes between the two writes, the order is saved in the database but the event is permanently lost from Kafka.
   The Outbox pattern writes the event into an `outbox` table in the **same ACID transaction** as the order. A background relay worker safely streams events to Kafka with at-least-once delivery.
2. **Why a Partial Index?**:
   In a production system with 10,000,000 orders, 9,999,950 rows are `PUBLISHED`, and only ~50 rows are `PENDING`.
   Indexing the entire table wastes 500 MB of RAM on dead rows. A partial index (`WHERE status IN ('PENDING', 'PROCESSING')`) indexes only active in-flight records, keeping the index tiny (kilobytes) and fitting entirely in CPU cache for instant $O(\log N)$ FIFO retrieval.

---

### 37. What is the difference between `*testing.T` and `*testing.B`? What does `BenchmarkStrategies` actually test?

- **`*testing.T` (Correctness)**: Runs code once to assert invariants (e.g. Zero Oversell: did exactly 50 succeed and 150 fail cleanly?).
- **`*testing.B` (Performance & Throughput)**: Runs code in an adaptive loop controlled by `b.N` across parallel goroutines (`b.RunParallel`) to measure nanoseconds per operation (`ns/op`) and throughput under sustained load.
- In `BenchmarkStrategies`, we give it plenty of stock (`int32(b.N + 1000)`) so the benchmark measures the **speed of executing complete ACID reservations**, rather than the speed of returning "out of stock" errors.

---

### 38. What does `<-blockChan` mean? What is the difference between an unbuffered channel and a buffered channel?

- **`<-blockChan`**: The receive operator. It pauses and freezes the goroutine until another goroutine sends data into `blockChan` or calls `close(blockChan)`.
- **Unbuffered Channel (`make(chan T)`)**: Zero storage capacity. The sender and receiver must meet at the exact same instant (synchronous hand-to-hand handoff). If no receiver is ready, the sender blocks.
- **Buffered Channel (`make(chan T, N)`)**: Holds up to $N$ items. Senders drop data in and continue immediately without waiting. Senders only block if all $N$ slots are full; receivers only block if the buffer is empty.
  - In Ware_Go, our **Semaphore** is a buffered channel (`make(chan struct{}, 100)`), allowing 100 concurrent requests to pass through without waiting.

---

### 39. What does `qtx := r.queries.WithTx(tx)` do?

`sqlc` queries normally run against the global connection pool (`pgxpool.Pool`).
`WithTx(tx)` binds the query methods to a specific open transaction `tx`.
Every query called on `qtx` participates in that single atomic transaction boundary. If `tx.Commit()` succeeds, all changes persist together; if any error occurs and `tx.Rollback()` runs, every change is undone cleanly.

---

### 40. Why limit the Reaper to 100 orders per batch instead of reclaiming all expired orders at once?

If 10,000 orders expire during a flash sale and the Reaper attempts to reclaim all 10,000 in a single giant query:
1. It holds row locks on warehouse inventory for several seconds.
2. Live customers attempting to checkout during those seconds freeze, suffering severe p99 latency spikes.
3. Database connections are hogged, risking connection pool exhaustion.

By bounding the batch to **100 orders**:
- The transaction commits in 10–15 milliseconds.
- Row lock hold times are negligible.
- Live customer checkouts are completely uninterrupted.

---

### 41. What if 10,000 orders expire at once during a flash sale? Shouldn't the Reaper be Adaptive instead of fixed at 100?

**Yes! In high-throughput architectures, a fixed sleep causes backlog accumulation.**
If 10,000 orders expire, waiting 1 second between 100-batches would take $10,000 / 100 = 100 \text{ seconds}$, breaching our 5-second reclaim SLA.

Production systems solve this using two adaptive patterns:
1. **Drain Loop (Zero-Sleep Backlog Draining)**:
   If a cycle reaped **exactly 100 orders** (`reapedCount == batchSize`), it signals that a backlog exists. The Reaper **does not sleep**. It immediately loops to fetch the next 100.
   Because each 100-batch takes ~10ms, it executes 50–70 batches per second, reclaiming 5,000–7,000 orders/sec and clearing 10,000 orders in **under 2 seconds**!
2. **Pool-Aware Dynamic Scaling**:
   The worker inspects `pool.Stat().IdleConns()`. If idle connections are high (e.g. 20 of 25 connections free), the batch size dynamically expands (e.g. to 250 or 500). If live traffic spikes and idle connections drop, the Reaper throttles its batch size to protect live customer traffic.

---

### 42. How does `for { select { ... } }` work in Go? Is it parallel or sequential, and how does Go decide between ready channels?

1. **Execution Model**: A single goroutine is **strictly sequential**. When `select` activates a case, the goroutine executes that case to completion before looping back. Incoming messages on other channels wait in their buffers.
2. **Channel Selection Rule**: If multiple channels have data ready simultaneously, Go **picks one uniformly at random**.
   - **Why? To prevent Channel Starvation (Priority Inversion)**. If Go evaluated cases in code order, a high-frequency channel on line 1 would permanently starve cases on lines 2 and 3. Random selection guarantees mathematical fairness.

---

### 43. What does `slog.Default().With("component", "reaper")` mean, and why use structured logging tags in production?

- `slog.Default()` returns the application's global JSON structured logger.
- `.With("component", "reaper")` creates a child logger that permanently attaches `"component": "reaper"` as a metadata attribute to every log entry.
- **Production Value**: Under 10,000 RPS, thousands of log lines fly by every second. In log aggregators (Datadog, Loki, CloudWatch), engineers can instantly isolate worker logs using queries like `component = "reaper" AND level = "ERROR"` without grepping through millions of unrelated HTTP logs.

---

### 44. What are the core components of a Kafka message (`Topic`, `Key`, `Value`, `Headers`, `Timestamp`), and what role does each play in Ware_Go?

A Kafka record consists of 5 fundamental parts:
1. **`Topic`** (`string`): The logical category/channel the message belongs to. In Ware_Go, we use the unified event stream `orders.events` so all lifecycle stages of an order land on the same stream.
2. **`Key`** (`[]byte`): The partition routing key. We set `Key = []byte(event.AggregateID)` (the `order_id`). Kafka hashes this key using Murmur2 to determine the exact partition. Because the key is identical across an order's lifecycle, `OrderReserved`, `OrderFulfilled`, and `OrderCancelled` are guaranteed to land on the **same partition and be consumed in strict chronological order**.
3. **`Value`** (`[]byte`): The main business payload (JSON binary data). Contains the full snapshot of customer ID, destination latitude/longitude, warehouse allocations, quantities, and prices.
4. **`Headers`** (`[]kafka.Header`): Key-value metadata attached to the envelope without deserializing the full body. We include:
   - `event_type`: e.g. `"OrderFulfilled"`, `"OrderCancelled"`. Downstream microservices inspect this header to route to specific handlers without parsing the entire JSON payload.
   - `aggregate_type`: e.g. `"ORDER"`. Identifies the domain aggregate root.
5. **`Time`** (`time.Time`): The creation timestamp, recording exactly when the event was emitted by the Outbox Relay. Downstream consumers use this to measure event propagation lag.

---

# Part 5: Load Testing, Observability & Performance Profiling (Week 6)

### 45. Why use k6 instead of Postman or JMeter for concurrency and load testing?

1. **Postman**: Postman is primarily designed for functional API verification and linear collection runs. It uses Node.js's single-threaded event loop and cannot easily simulate hundreds of real, simultaneous OS threads or goroutines firing requests at the exact same microsecond. It is not built for stress testing high-concurrency race conditions.
2. **Apache JMeter**: While capable of high load, JMeter follows an OS thread-per-virtual-user model written in Java. Simulating 1,000+ VUs consumes gigabytes of JVM memory, incurs heavy garbage collection pauses, and requires complex XML configuration files.
3. **Grafana k6 (Our Choice)**:
   - Written in **Go**, compiling directly to a native machine executable with near-zero runtime overhead.
   - Each Virtual User (VU) runs inside a lightweight Go goroutine rather than an OS thread, allowing thousands of concurrent users to run smoothly on a standard laptop.
   - Test scenarios are scripted in modern JavaScript/ES6.
   - Built-in **Thresholds** allow automated mathematical assertions directly in terminal / CI/CD (e.g. `p(99) < 250ms`, `rate == 0`).

---

### 46. Why did our k6 script fail initially with a database error on warehouse code `VARCHAR(20)`?

In `s1_zero_oversell.js`, the setup function seeded temporary warehouses and generated warehouse codes dynamically:
```javascript
const whCode = `WH-FLASH-${Date.now()}`;
```
`Date.now()` produces a 13-digit Unix millisecond timestamp (e.g., `1794451234567`).  
Concatenated with `WH-FLASH-`, the string length was:
`9 + 13 = 22 characters`.

However, our PostgreSQL database schema in `001_init.sql` strictly enforces:
```sql
code VARCHAR(20) NOT NULL UNIQUE
```
PostgreSQL rejected the insert with SQLSTATE `22001` (`string_data_right_truncation`).

**The Fix**:  
Replaced timestamp concatenation with a bounded random 5-digit integer:
```javascript
const rand = Math.floor(10000 + Math.random() * 90000);
const whCode = `WH-${rand}`; // e.g. "WH-48291" (8 chars <= 20)
```
This taught an essential production lesson: test harnesses must strictly honor database column constraint boundaries.

---

### 47. Why did `s1_zero_oversell.js` require unique Idempotency Keys across VUs?

In Ware_Go, we built an Idempotency Middleware in Week 3 that caches every successful response in Redis by `Idempotency-Key`.

If our load test script had sent the same hardcoded `Idempotency-Key: test-flash-sale` across all 500 Virtual Users:
1. VU #1 would hit PostgreSQL, acquire row locks, and reserve 1 unit of stock.
2. VUs #2 through #500 would hit Redis, find the cached HTTP 200 response, and immediately return `{"status":"RESERVED"}` with `X-Idempotent-Replay: true`.
3. **The bug**: 500 requests would report HTTP 200 success, but only 1 unit was ever deducted in PostgreSQL! 99 units would remain in the warehouse while the test falsely reported 500 orders reserved.

**The Solution**:  
Each concurrent VU generates a unique UUID v4 as its `Idempotency-Key`:
```javascript
"Idempotency-Key": `order-s1-${__VU}-${__ITER}-${Date.now()}`
```
This forces all 500 VUs to bypass the replay cache and aggressively compete for database row locks simultaneously, creating the exact race condition needed to prove Claim 1 (Zero Oversell).

---

### 48. How does the Reaper SLA test (`s2_zombie_stock.js`) work with fast TTLs?

In standard operations, unpaid order reservations expire after 10 or 15 minutes. However, waiting 15 minutes in an automated load test is completely impractical.

**The Fast-TTL Test Mechanism**:
1. `s2_zombie_stock.js` seeds a warehouse with 100 units of stock.
2. It reserves 50 units while passing an explicit short expiration override (`expires_in_seconds: 2`).
3. Database `available_qty` drops from 100 to 50, and `reserved_qty` rises to 50.
4. The test records the exact millisecond expiration threshold: $T_{\text{expire}} = T_{\text{reserve}} + 2000\text{ms}$.
5. The test polls the stock endpoint every 200ms until `available_qty` returns to 100.
6. The measured duration $T_{\text{recovery}} - T_{\text{expire}}$ was **2,731 ms**.
7. Because $2.731\text{s} \le 5.0\text{s}$, it empirically proved Claim 4: abandoned zombie inventory is reclaimed within the 5.0-second SLA.

---

### 49. How does Scenario 3 (`s3_deadlock.js`) prove deadlock freedom under opposing cross-warehouse orders?

A classic database deadlock occurs when two transactions attempt to lock the same resources in opposing order:
- **Transaction A**: Locks Warehouse 1, then attempts to lock Warehouse 2.
- **Transaction B**: Locks Warehouse 2, then attempts to lock Warehouse 1.
- Both transactions freeze indefinitely waiting for each other. PostgreSQL's deadlock detector eventually detects the cycle and aborts one transaction with error:  
  `ERROR: deadlock detected (SQLSTATE 40P01)`.

**The Proof in `s3_deadlock.js`**:
1. We spun up 200 concurrent VUs dividing into two adversarial groups:
   - Group Even: requested items in order `[Warehouse-A, Warehouse-B]`.
   - Group Odd: requested items in reverse order `[Warehouse-B, Warehouse-A]`.
2. Because Ware_Go enforces the **Global Lock Hierarchy** in Go before opening the database transaction:
   ```go
   sort.Slice(warehouses, func(i, j int) bool {
       return warehouses[i].ID < warehouses[j].ID
   })
   ```
3. Even though Group Odd sent the request in reverse order, Go sorted the IDs deterministically before issuing SQL `SELECT ... FOR UPDATE`.
4. Result: 200 VUs competed simultaneously across opposing carts with **0 deadlocks** (`deadlock_detected = 0`), proving Claim 3.

---

### 50. How does Scenario 4 (`s4_latency.js`) measure latency curves under sustained load?

Instead of generating arbitrary bursts, `s4_latency.js` uses k6's `constant-arrival-rate` executor:
```javascript
executor: 'constant-arrival-rate',
rate: 500,
timeUnit: '1s',
duration: '20s',
preAllocatedVUs: 100,
maxVUs: 200,
```
- It drives a strict, constant 500 requests per second into the system for 20 seconds (10,000 total requests).
- It measures p50 (median), p95 (95th percentile), and p99 (99th percentile) latency.
- It proves Claim 2: cache hits return in $< 1\text{ms}$ from Redis, while full ACID reservation transactions via PostgreSQL commit with a p95 under 100ms and p99 under 250ms, without latency degradation under sustained load.

---

### 51. How does the Semaphore Load Shedder prevent Cascading Failures during traffic surges?

When incoming traffic surges past system capacity (e.g. 10,000 requests per second hit a server sized for 2,000 RPS):
1. **Without Load Shedding**: All 10,000 requests open database connections, fill goroutine stacks, and saturate CPU memory. The database connection pool is exhausted, response times blow past 15 seconds, client timeouts trigger retries, and the entire system collapses into an unresponsive crash (Cascading Failure / Death Spiral).
2. **With Ware_Go's Bounded Wait Semaphore**:
   - We allocate a buffered channel of 100 tokens: `make(chan struct{}, 100)`.
   - Only 100 concurrent requests may execute database transactions at any millisecond.
   - Request #101 does not crash; it waits in a bounded window (`waitTimeout = 150ms`).
   - If an active slot frees up within 150ms, request #101 grabs the slot and executes.
   - If 150ms passes and all 100 slots remain saturated, the request is immediately rejected with:  
     `HTTP 503 Service Unavailable: server overloaded, please retry`.
3. **The Result**: PostgreSQL never receives more than 100 concurrent transactions. Throughput remains near peak capacity, latencies stay bounded, and the server recovers instantaneously as soon as traffic subsides.

---

### 52. Prometheus & Grafana Architecture: How does metric collection work in Ware_Go without slowing down requests?

In high-concurrency systems, logging or recording metrics to disk or database inside HTTP handlers adds latency and disk I/O contention.

**How Ware_Go Solves This**:
1. **In-Memory Lock-Free Collection**:
   The `promhttp` library maintains metric counters and histogram buckets as in-memory atomic 64-bit floating point numbers. Recording an HTTP request duration takes less than 20 nanoseconds (CPU cache operation) with zero locks.
2. **Pull-Based Architecture**:
   Prometheus scrapes the `/metrics` endpoint on an independent background timer (e.g. every 5 seconds). HTTP handlers never push data over the network.
3. **Grafana Dashboards**:
   Grafana connects to Prometheus as a data source and executes PromQL queries (e.g. `rate(http_requests_total[1m])`, `histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[1m])) by (le))`) to visualize real-time RPS, error rates, p95/p99 latency curves, and worker queue depths.

---

### 53. How does Go `pprof` profiling work, and how do we capture CPU, heap, and goroutine profiles?

Go includes an industrial-grade runtime profiler built directly into the standard library (`net/http/pprof`).

1. **Mounting Endpoints**:
   In `internal/api/router.go`, we mount `/debug/pprof/` routes:
   - `/debug/pprof/profile`: CPU execution profiling.
   - `/debug/pprof/heap`: Memory allocation and live heap objects.
   - `/debug/pprof/goroutine`: Stack traces of all running goroutines (detects goroutine leaks).
2. **Capturing a 30-Second CPU Profile**:
   ```bash
   go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30
   ```
   During this window, Go interrupts the CPU at 100Hz to sample instruction pointers across all OS threads.
3. **Analyzing in the Web UI / Flame Graph**:
   ```bash
   go tool pprof -http=:8081 cpu.pprof
   ```
   This generates an interactive Flame Graph showing exactly which functions (e.g. JSON serialization, pgx connection acquisition, SQL query execution) consume the most CPU cycles.

---

# Part 6: AI Inventory Rebalancer, Quota Optimization & Production Architecture (Week 7)

### 54. What is the AI Inventory Rebalancing Agent, and why not use pure rule-based logic?

1. **The Problem**: Over time, regional fulfillment centers become unbalanced. Warehouse A may have 500 units of a laptop with 2 orders per week, while Warehouse B has only 5 units with 50 orders per week and faces impending stockouts.
2. **Why Not Pure Rule-Based Logic?**:
   - Hardcoded if-else heuristics (e.g., `if stock < 10 then transfer from highest`) fail to consider complex multi-variable business context: transportation distances, carrying costs, SKU turnover velocity, regional seasonal spikes, and warehouse storage capacity constraints.
3. **The Ware_Go Hybrid Solution**:
   - Our system fetches live warehouse stock levels and coordinates via PostgreSQL.
   - It submits the structured snapshot to **Google Gemini 3.5 Flash Lite** with a strict JSON schema prompt.
   - Gemini evaluates the network topology, computes optimal stock transfer deltas, and produces:
     1. Structured transfer steps (`source_warehouse_id`, `target_warehouse_id`, `sku`, `quantity`).
     2. A human-readable business justification (`analysis`) explaining the economic and operational rationale.

---

### 55. Model Lifecycle & Free Tier Quota Economics: Why did we switch from Gemini 1.5/2.0 to 3.8 and finally to 3.5 Flash Lite?

During Week 7 development, we encountered critical real-world LLM lifecycle dynamics:

1. **`gemini-1.5-flash` / `gemini-2.0`**:
   Google retired / deprecated these endpoints, returning:  
   `Error 404: models/gemini-2.5-flash is no longer available`.
2. **`gemini-3.8-flash`**:
   Upgrading to `gemini-3.8-flash` succeeded initially, but quickly hit:  
   `Error 429: Resource has been exhausted (check quota and billing)`.  
   **The Discovery**: Gemini 3.8 Flash Free Tier has extremely tight limits: **5 Requests Per Minute (RPM)** and **20 Requests Per Day (RPD)**. A single retry or test burst immediately exhausts the entire daily quota.
3. **`gemini-3.5-flash-lite` (The Production Winner)**:
   Google offers generous developer tier allocations for `gemini-3.5-flash-lite`:
   - **15 Requests Per Minute** (3x higher throughput).
   - **500 Requests Per Day** (25x higher daily quota).
   - Ultra-fast token latency ($< 1.5\text{s}$ response time).
   Switching to `gemini-3.5-flash-lite` gave us high availability, deterministic schema generation, and zero rate limit errors.

---

### 56. Dynamic LLM Configuration: Why make the model name configurable in `.env`?

Hardcoding model identifiers like `const ModelName = "gemini-3.5-flash-lite"` inside Go source code is a major anti-pattern:
- LLM providers update, alias, and deprecate model IDs on rapid release cycles.
- If a model is deprecated, a hardcoded application crashes in production with HTTP 404 until engineers commit code, build binaries, and redeploy containers.

**Our Implementation in `internal/config/config.go`**:
```go
GeminiModel: getEnv("GEMINI_MODEL", "gemini-3.5-flash-lite"),
```
By making `GEMINI_MODEL` configurable via `.env` with a reliable fallback, DevOps can switch models instantly in production without recompiling or redeploying code.

---

### 57. Why did `/api/v1/agent/proposals` fail initially with `curl (52) Empty reply from server`?

When testing `POST /api/v1/agent/proposals`, the curl client froze for 10 seconds and aborted with:
`curl: (52) Empty reply from server`.

**The Root Cause**:
In `cmd/server/main.go`, our HTTP server had an aggressive write timeout:
```go
server := &http.Server{
    Addr:         ":" + cfg.Port,
    Handler:      router,
    ReadTimeout:  10 * time.Second,
    WriteTimeout: 10 * time.Second, // <-- The Culprit!
}
```
External LLM calls over the public internet (connecting to `generativelanguage.googleapis.com`, generating tokens, returning JSON) typically take **12 to 18 seconds** on cold start or network jitter.  
At second 10, Go's HTTP server fired the `WriteTimeout` deadline and **forcefully closed the underlying TCP connection**, dropping the client before the handler could write the HTTP response.

**The Fix**:  
Increased `WriteTimeout` and `ReadTimeout` to `60 * time.Second` on `http.Server`. The client received the full HTTP 200 JSON payload cleanly.

---

### 58. Stateful Proposal Execution & Two-Phase Safety: Why store AI proposals in PostgreSQL before execution?

Allowing an AI model to execute physical inventory transfers directly in a single step is unsafe and violates enterprise audit compliance.

**Our Two-Phase Stateful Architecture**:
1. **Phase 1: Generation & Persistence (`POST /api/v1/agent/proposals`)**:
   - LLM generates transfer proposals.
   - We write the proposal into PostgreSQL table `rebalance_proposals` with:
     - `status = 'PENDING'`
     - `analysis = TEXT`
     - `proposals_json = JSONB`
     - `created_at = NOW()`
   - An operations manager or inventory lead can review the proposal.
2. **Phase 2: Atomic Execution (`POST /api/v1/agent/execute-transfer`)**:
   - Reads the latest proposal from PostgreSQL.
   - Validates that `status == 'PENDING'`.
   - Executes stock deduction from the source warehouse and addition to the target warehouse inside a **single ACID transaction**.
   - Emits immutable accounting entries (`TRANSFER_OUT`, `TRANSFER_IN`) in `inventory_audit_log`.
   - Updates `status = 'EXECUTED'` and `executed_at = NOW()`.
3. **Duplicate Prevention**:
   - If an engineer or cron job triggers `POST /api/v1/agent/execute-transfer` a second time, the database checks status and immediately rejects the call with:  
     `HTTP 409 Conflict: the latest rebalance proposal has already been executed`.
   - This mathematically prevents double-transfers or duplicate inventory deductions.

---

### 59. Multi-Stage Dockerfile: Why do we use a multi-stage build, and how does it shrink image size from 1+ GB to 18.1 MB?

A standard single-stage Dockerfile builds and runs in the same image:
```dockerfile
FROM golang:1.24
COPY . .
RUN go build -o /app/server cmd/server/main.go
CMD ["/app/server"]
```
This single image bundles the Go compiler, standard library source files, git, C build tools, and layer caches, resulting in an image size of **over 1.2 Gigabytes**.

**Our Production Multi-Stage Strategy (`Dockerfile`)**:
1. **Stage 1 (Builder)**:
   - Uses `golang:alpine`.
   - Compiles the binary using:
     ```bash
     CGO_ENABLED=0 GOOS=linux go build -mod=vendor -ldflags="-s -w" -o /app/warego-server ./cmd/server
     ```
   - `-ldflags="-s -w"` strips all debugging information and DWARF symbol tables, shaving 40% off the executable binary size.
   - `CGO_ENABLED=0` produces a 100% statically linked binary with zero dependencies on external C runtime libraries (`glibc`).
2. **Stage 2 (Minimal Runner)**:
   - Uses `alpine:latest` (~7 MB base).
   - Adds only root CA certificates (`ca-certificates`) and `curl` for container healthchecks.
   - Copies **ONLY the single compiled binary** (`/app/warego-server`) from the builder stage.
   - Completely discards Go source code, compilers, build caches, and tools.
3. **The Result**:
   - Final Docker Content Size: **18.1 Megabytes**!
   - 98.5% smaller than standard images, blazingly fast to pull across Kubernetes clusters, and minimal attack surface.

---

### 60. Unprivileged User Security in Docker: Why create `appuser` and `appgroup`?

By default, Docker containers run all processes as the `root` superuser (UID 0).

**The Security Risk**:
If an attacker discovers a remote code execution (RCE) vulnerability in an HTTP endpoint or third-party dependency:
1. They execute commands inside the container as `root`.
2. Any volume mounted from the host (such as Docker sockets or file shares) can be rewritten with superuser authority.
3. If a container breakout vulnerability exists in the Linux kernel, the attacker gains direct root access to the physical host node.

**Our Defense**:
In Stage 2 of `Dockerfile`:
```dockerfile
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
```
The application runs as unprivileged user `appuser` (UID 10001). Even if the application process were compromised, it cannot modify system packages, mount devices, or escape container boundaries.

---

### 61. Why did `RUN go mod download` time out in Docker, and why is `go mod vendor` the production solution?

During Docker image build on Windows, line 9 of the Dockerfile failed after 575 seconds:
```
=> ERROR [builder 5/7] RUN go mod download:
stream error: stream ID 231; INTERNAL_ERROR; received from peer
read "https://proxy.golang.org/...": unexpected EOF
```

**Why This Happened**:
1. `RUN go mod download` makes dozens of external HTTPS requests to `proxy.golang.org` to download tarballs of all project dependencies.
2. In constrained, firewalled, or intermittent corporate/home network environments, large dependency downloads drop sockets and time out.
3. Depending on external internet access during a container build makes CI/CD builds non-deterministic, slow, and fragile.

**The Production Fix: Go Vendoring**:
1. Run `go mod vendor` on the host machine.
2. This downloads and stores the exact source code of every dependency into a local `vendor/` directory.
3. In `Dockerfile`, replace `RUN go mod download` with:
   ```dockerfile
   RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -ldflags="-s -w" -o /app/warego-server ./cmd/server
   ```
4. **Benefits**:
   - Build time plummeted from 575s (failure) to **30.1 seconds** (success).
   - Builds are **100% offline**, reproducible, and immune to proxy timeouts or third-party package outages.

---

### 62. Docker Images: What is the difference between `DISK USAGE` and `CONTENT SIZE`?

When inspecting our image using `docker images ware_go:latest`:
```
IMAGE            ID             DISK USAGE   CONTENT SIZE
ware_go:latest   0a3dc62ff9d9       67.7MB         18.1MB
```

1. **`CONTENT SIZE` (18.1 MB)**:
   - The actual uncompressed size of the root filesystem layers contained in the image (the Alpine OS files + our compiled Go binary).
   - This is the true metric of image efficiency. When pushing to Docker Hub or AWS ECR, only these compressed layers (~9 MB over the wire) are transferred.
2. **`DISK USAGE` (67.7 MB)**:
   - The physical disk space currently consumed on your local machine by Docker's storage driver (`overlay2`).
   - Includes local filesystem metadata, snapshot diff trees, inode allocations, and build cache references linked to that image on the Docker daemon.

---

### 63. Horizontal Scaling vs Database Bottleneck: Does adding more containers help when 10,000 requests hit PostgreSQL?

**The User's Scenario**:
*"If 10,000 requests hit our service, would running 5 containers instead of 1 help? Since PostgreSQL does row locking (`FOR UPDATE`), won't PostgreSQL always be the bottleneck?"*

**The Deep Answer**:
1. **For Hot SKUs (Flash Sales)**:
   - **No, adding more containers does NOT increase throughput for a single hot item!**
   - If 10,000 requests are competing to reserve the same SKU (e.g. `IPHONE-16`), PostgreSQL must serialize them using row-level locks (`SELECT ... FOR UPDATE`).
   - Whether those 10,000 requests arrive through 1 container or 10 containers, they all queue up behind the exact same database row lock in PostgreSQL.
   - In fact, adding 5 containers can **degrade performance**: each container opens 20 database connections, totaling 100 concurrent connections. PostgreSQL must now juggle 100 OS worker processes, causing excessive CPU context switching and lock contention!
2. **When Does Adding Containers Help?**:
   - **Stateless Workloads**: JWT token verification, JSON serialization, request validation, Haversine geospatial calculations.
   - **Cache Hits**: Requests served directly from Redis memory (`GET /api/v1/inventory/:sku`). 5 containers can serve 100,000+ cached RPS easily without touching PostgreSQL.
   - **Cold SKUs**: If 10,000 requests target 500 different products across 20 warehouses, row locks do not conflict. Multiple containers help saturate database I/O across independent rows.
3. **How Do High-Scale Systems (Amazon, Flipkart) Solve the Flash Sale Database Bottleneck?**:
   - **In-Memory Atomic Decrement (Redis + Lua Scripting)**: Decrement inventory in Redis RAM at 100,000 RPS. PostgreSQL is only updated asynchronously via message queues.
   - **PgBouncer**: Sits between app containers and PostgreSQL to pool transactions and eliminate connection churn.
   - **Sharding & Partitioning**: Splitting inventory tables by Warehouse ID or Product Category across multiple independent database servers.

---

### 64. Single-VM Architecture: Can we run Nginx, App, Postgres, Redis, and Kafka all on one virtual machine? How does traffic distribution work?

**Yes! For staging, portfolios, or small/medium workloads, running all containers on a single Linux cloud VM (e.g. Azure B2PTSV2) is common practice.**

**How It Works Architecturally**:
```
                      Internet Traffic (:80 / :443)
                                  │
                                  ▼
                     ┌───────────────────────────┐
                     │   Nginx (Reverse Proxy)   │
                     └─────────────┬─────────────┘
                                   │ (Round-Robin Load Balancing)
            ┌──────────────────────┴──────────────────────┐
            ▼                                             ▼
  ┌───────────────────┐                         ┌───────────────────┐
  │  ware_go_app_1    │                         │  ware_go_app_2    │
  └─────────┬─────────┘                         └─────────┬─────────┘
            │                                             │
            └──────────────┬──────────────────────────────┘
                           │ (ware_go_net Docker Bridge)
       ┌───────────────────┼───────────────────┐
       ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  PostgreSQL  │    │  Redis 7     │    │  Redpanda    │
│  (DB Engine) │    │  (Cache)     │    │  (Kafka Bus) │
└──────────────┘    └──────────────┘    └──────────────┘
```

1. **Traffic Ingestion**: Nginx binds to host ports 80/443, handles TLS/SSL certificates, and proxies HTTP requests to `http://ware_go_app_1:8080` and `http://ware_go_app_2:8080` using round-robin.
2. **Container Communication**: All containers join a single Docker network (`ware_go_net`). The app containers connect to `postgres:5432`, `redis:6379`, and `redpanda:29092` via Docker's internal DNS.
3. **Resource Constraint Warning**:
   - While functionally complete, running Kafka/Redpanda, PostgreSQL, Redis, Nginx, and Go on a machine with **only 1 GB or 2 GB of RAM** will easily encounter Linux Out-Of-Memory (`OOMKilled`) termination under load. Redpanda and Postgres both prefer at least 1 GB to 2 GB of dedicated memory.

---

### 65. Managed Cloud Services vs Self-Hosted in `docker-compose.yml`: How do external Database URLs work?

If you decide to use cloud-managed services (e.g. AWS RDS for Postgres, Upstash for Redis, Confluent Cloud for Kafka):

1. **What to Remove from `docker-compose.yml`**:
   You simply remove the container service definitions for `postgres:`, `redis:`, and `redpanda:`:
   - Docker no longer pulls or runs those heavy images locally.
   - You only run the lightweight `app` container.
2. **How the Application Connects**:
   The application does not care whether PostgreSQL is running inside Docker or in an AWS data center in Virginia.
   You simply supply the managed connection URIs in your `.env` file:
   ```env
   DATABASE_URL=postgres://master:secret@prod-db.xyz.rds.amazonaws.com:5432/ware_go?sslmode=require
   REDIS_URL=redis://default:token@east-redis.upstash.io:6379
   KAFKA_BROKERS=pkc-xyz.us-east-1.aws.confluent.cloud:9092
   ```
3. **Network Behavior**:
   The Go container reaches the internet through the Docker host gateway, establishing secure TLS connections directly to your managed cloud providers.

---

### 66. Docker Networks: What does `networks: ware_go_net` and `driver: bridge` mean?

In `docker-compose.prod.yml`:
```yaml
networks:
  ware_go_net:
    driver: bridge
```

1. **Declaring a New Network**:
   This tells the Docker daemon to instantiate a private, isolated virtual software switch (bridge) on the host system specifically for Ware_Go containers.
2. **Bridge Driver**:
   - In Linux networking, a "bridge" is a software device that connects two or more network segments together.
   - It assigns private IP addresses (e.g. `172.20.0.x`) to each container attached to it.
3. **Why Not Use the Default Docker Network?**:
   - In custom user-defined bridge networks, Docker enables **Automatic Service Discovery via DNS**.
   - A container can connect to PostgreSQL using `host=postgres` rather than tracking changing IP addresses.
   - External machines on your home Wi-Fi or local network cannot reach the internal database or cache ports unless explicitly published with `ports:`, providing robust network isolation.
