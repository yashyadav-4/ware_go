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
