# Predix Reliability & Distributed Matching Engine
## Implementation Specification

**Repository:** `Manav2209/golang-predix`  
**Target branch:** `main`  
**Scope:** Repair the current single-node system, establish durable event contracts, then introduce partitioned distributed engine ownership.

---

# 1. Objective

Transform Predix from the current partially connected microservice skeleton into a reliable prediction-market backend with:

- deterministic order matching
- durable command processing
- idempotent order commands
- reliable trade settlement
- reliable WebSocket updates
- crash recovery
- partition-based event ownership
- single active engine owner per partition
- failover and state reconstruction
- production-grade observability and tests

The implementation MUST follow the dependency order:

```text
P0 → P1 → P2 → P3
```

Do not begin distributed engine work until P0 is working end-to-end.

The current architecture already has separate API, engine, DB-worker, and WebSocket services, but the review found that their contracts are currently disconnected.

---

# 2. Current Architecture

Current high-level flow:

```text
API
 │
 ▼
Redis "messages"
 │
 ▼
Engine
 ├── in-memory orderbook
 ├── WAL
 ├── Redis pub/sub
 │
 ├──────────────► WebSocket
 │
 └──────────────► ??? trade settlement

DB Worker
 │
 └── BRPOP "db_processor"
```

The critical problem is that the engine publishes trades through Redis Pub/Sub while the DB worker consumes `db_processor` through Redis lists. Nothing currently bridges the two transports.

---

# 3. Target Architecture

After P0:

```text
                    ┌──────────────┐
                    │     API      │
                    └──────┬───────┘
                           │
                     command envelope
                           │
                           ▼
                    ┌──────────────┐
                    │    Redis     │
                    │ command log  │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Engine    │
                    │              │
                    │ Orderbook    │
                    │ Matching     │
                    │ State        │
                    └──────┬───────┘
                           │
                    EventEnvelope
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
       ┌──────────┐                ┌──────────┐
       │ DB Worker│                │    WS    │
       └────┬─────┘                └──────────┘
            │
            ▼
        PostgreSQL
```

After P2:

```text
                        API
                         │
                  hash(eventId)
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
 commands:p0      commands:p1      commands:pN
        │                │                │
        ▼                ▼                ▼
 Engine A          Engine B          Engine C
 owns p0           owns p1           owns pN
        │                │                │
        └────────────────┼────────────────┘
                         ▼
                 Event / Outbox Log
                     │        │
                     ▼        ▼
                  DB Worker    WS
```

Each event belongs to exactly one partition.

---

# 4. Non-Goals

This specification does not include:

- frontend changes
- trading strategy logic
- cross-event matching
- multi-region deployment
- Kubernetes deployment
- horizontal DB sharding
- advanced market types beyond the current matching model

---

# 5. P0 — End-to-End Single Node

## P0.1 Unified Event Envelope

Create one canonical event structure shared by Engine, DB Worker and WebSocket.

Suggested location:

```text
internal/events/envelope.go
```

Schema:

```go
type EventEnvelope struct {
    Type        string          `json:"type"`
    EventID     string          `json:"eventId,omitempty"`
    PartitionID int             `json:"partitionId,omitempty"`
    Sequence    uint64          `json:"sequence"`
    Timestamp   time.Time       `json:"timestamp"`
    Data        json.RawMessage `json:"data"`
}
```

Initial event types:

```go
const (
    EventTradeExecuted = "trade_executed"
    EventOrderCreated  = "order_created"
    EventOrderCanceled = "order_canceled"
    EventOrderUpdated  = "order_updated"
    EventDepthUpdated  = "depth_updated"
)
```

Requirements:

- all downstream events MUST use the envelope
- `Type` MUST never be inferred from the embedded payload
- DB Worker MUST deserialize the same schema
- WebSocket MUST deserialize the same schema
- event versioning should be possible later

---

# 6. P0.2 Engine → DB Worker Pipeline

Replace the current raw Redis Pub/Sub trade-only pipeline.

On trade execution:

```text
match
 ↓
create EventEnvelope
 ↓
durably enqueue event
 ↓
DB Worker consumes event
 ↓
settle trade
```

The engine MUST send `trade_executed` to:

```text
db_processor
```

using the same `EventEnvelope` consumed by DB Worker.

The existing DB Worker already expects an envelope-style message, but the engine currently does not feed that queue. 

Implementation:

```go
func (e *Engine) publishTrade(trade *Trade) error {
    envelope := EventEnvelope{
        Type:      EventTradeExecuted,
        EventID:   trade.EventID,
        Timestamp: time.Now().UTC(),
        Data:      mustMarshal(trade),
    }

    return enqueueDBEvent(envelope)
}
```

Do not use Pub/Sub as the only settlement transport.

---

# 7. P0.3 WebSocket Event Contract

WebSocket consumers MUST receive:

```json
{
  "type": "trade_executed",
  "eventId": "event-123",
  "sequence": 1842,
  "data": {
    "id": "trade-123",
    "orderId": "order-a",
    "matchOrderId": "order-b",
    "quantity": 10,
    "price": 0.62
  }
}
```

Current behavior sends raw `Trade` JSON, while the WebSocket parser expects `{type,data}`.

Implementation requirements:

- update `publishTrade`
- WebSocket Redis subscriber consumes `EventEnvelope`
- unknown event type must produce a structured error
- malformed events must not crash the subscriber
- add WebSocket event tests

---

# 8. P0.4 Fix DB Worker / SQLC Integration

Bring DB Worker and generated SQLC code into agreement.

Required operations:

```text
InsertTrade
UpdateOrderFill
UpdateOrderStatus
IncrementEventVolume
DeductBalance
CreditBalance
```

The review identified the current SQLC mismatches, including missing `ApplyOrderFill`, missing `IncrementEventVolume`, invalid `DB()` usage and parameter mismatches.

Do not work around generated SQL code from application code.

Instead:

1. update SQL queries
2. regenerate SQLC
3. update worker against generated types
4. compile
5. add settlement tests

---

# 9. P0.5 Trade Settlement Transaction

A single trade settlement MUST be atomic.

Transaction:

```text
BEGIN

insert trade

update taker order
update maker order

debit buyer
credit seller

increment event volume

COMMIT
```

On any failure:

```text
ROLLBACK
```

Settlement MUST be idempotent using:

```text
trade.id
```

A duplicate `trade_executed` event MUST NOT create duplicate balances or trades.

---

# 10. P0.6 Persist Orders Correctly

Current order creation sends the command to the engine but does not persist the order first, while cancellation requires a DB record.

Use client-generated IDs:

```json
{
  "commandId": "uuid",
  "orderId": "uuid",
  "eventId": "event-123"
}
```

Do not generate a replacement order ID inside the engine when the API already has one.

Required behavior:

```text
client sends orderId=A
        ↓
engine processes
        ↓
retry orderId=A
        ↓
same order/result returned
```

Duplicate order IDs MUST NOT create another order.

---

# 11. P0.7 Order Lifecycle

Define canonical statuses:

```go
PENDING
PARTIAL
FILLED
CANCELED
REJECTED
```

Choose one spelling and use it everywhere.

Recommended:

```text
CANCELED
```

Update:

- engine
- SQL schema/check constraints
- API
- DB worker
- WebSocket
- tests

No service may use a different spelling.

The review currently identifies `CANCELED` vs `CANCELLED` as a mismatch.

---

# 12. P0.8 Fix Orderbook Endpoint

`GET /orderbook/:eventId` MUST call:

```text
GET_DEPTH
```

not:

```text
GET_OPEN_ORDERS
```

Response should contain:

```json
{
  "eventId": "event-123",
  "bids": [],
  "asks": []
}
```

Include:

```text
price
quantity
orderCount
```

where appropriate.

---

# 13. P0.9 Fix Engine Shutdown

Current shutdown ordering causes a deadlock because `pendingQueue` is closed only after `wg.Wait()`, while `processOrders` waits for that channel to close.

Target behavior:

```text
shutdown signal
      ↓
cancel context
      ↓
stop Redis consumer
      ↓
close pending queue
      ↓
wait for workers
      ↓
flush WAL/outbox
      ↓
close Redis
      ↓
exit
```

`Shutdown(ctx)` MUST honor the provided context timeout.

---

# 14. P0 Acceptance Criteria

P0 is complete only when:

- an order can be created
- an order can be matched
- a trade reaches DB Worker
- trade is persisted
- balances settle atomically
- WebSocket receives the trade event
- duplicate order commands are idempotent
- duplicate trade events are idempotent
- cancellation works
- orderbook endpoint returns market depth
- engine shuts down without hanging
- repository builds successfully
- all new tests pass

End-to-end test:

```text
Create event
   ↓
Create BUY
   ↓
Create SELL
   ↓
Engine matches
   ↓
Trade event generated
   ↓
DB settlement
   ↓
WS trade event
   ↓
GET order
   ↓
FILLED
```

---

# 15. P1 — Matching Correctness

## P1.1 Deterministic Match Response

Current behavior returns `PENDING` before background matching completes, which means clients can observe stale state.

Preferred implementation:

```text
command
 ↓
engine processes command
 ↓
matching completes
 ↓
response contains resulting state
```

Example:

```json
{
  "orderId": "order-123",
  "status": "FILLED",
  "filledQuantity": 10,
  "remainingQuantity": 0
}
```

For longer operations, use a command response plus event stream, but state transitions MUST remain observable.

---

# 16. P1.2 Self-Trade Prevention

Before matching:

```go
if incoming.UserID == resting.UserID {
    do not match
}
```

Define explicit behavior:

```text
cancel incoming
cancel resting
skip resting order
reject incoming
```

Pick one policy and document it.

No order may execute against the same user's resting order.

---

# 17. P1.3 Balance and Position Validation

Before accepting an order:

For BUY:

```text
requiredBalance >= quantity * price
```

For SELL:

```text
ownedPosition >= quantity
```

Funds/positions must be reserved so multiple concurrent orders cannot over-commit the same balance.

Introduce:

```text
available balance
reserved balance
available position
reserved position
```

Settlement releases or consumes reservations.

---

# 18. P1.4 Replace float64 for Financial Values

Do not use `float64` for monetary values.

Current engine models use floating-point quantities/prices.

Preferred approach:

```text
decimal
```

or fixed-point integer units.

Example:

```go
type Price int64
type Quantity int64
```

with documented precision.

The same representation must be used across:

```text
API
Engine
DB
Settlement
Trade events
```

---

# 19. P1.5 Durable Command Log

WAL requirements:

```text
append
fsync
replay
checkpoint
```

If retaining local WAL temporarily:

```go
file.Write(...)
file.Sync()
```

But the recommended long-term design is a partitioned Redis Streams command log.

Command format:

```json
{
  "commandId": "uuid",
  "type": "CREATE_ORDER",
  "eventId": "event-123",
  "sequence": 1234,
  "timestamp": "...",
  "payload": {}
}
```

The command log becomes the source of truth for engine reconstruction.

---

# 20. P1.6 Engine State Recovery

On startup:

```text
load partition
 ↓
read command log
 ↓
replay commands in sequence order
 ↓
rebuild markets
 ↓
rebuild orderbooks
 ↓
restore order states
 ↓
start processing new commands
```

Replay MUST be deterministic.

Running the same command sequence twice must result in the same engine state.

---

# 21. P1.7 Event Recovery / Outbox

Trade publication must not rely on a best-effort Redis publish.

Current behavior logs failed trade publication and continues, which can leave memory ahead of DB/WS.

Target:

```text
match
 ↓
create trade
 ↓
write durable outbox
 ↓
publish event
 ↓
mark published
```

On restart:

```text
read unpublished outbox events
 ↓
publish
 ↓
mark published
```

No trade may disappear because Redis was temporarily unavailable.

---

# 22. P1 Acceptance Criteria

P1 is complete when:

- duplicate commands are safe
- duplicate trade events are safe
- self-trades are impossible
- insufficient balance is rejected
- insufficient position is rejected
- financial values do not use floating-point arithmetic
- engine can restart and reconstruct state
- failed event publishing is recoverable
- order state after a command is deterministic

---

# 23. P2 — Distributed Engine

## P2.1 Partitioning

Partition events using:

```text
partitionId = hash(eventId) % N
```

Command stream:

```text
commands:partition:{partitionId}
```

For example:

```text
commands:partition:0
commands:partition:1
commands:partition:2
...
```

All commands affecting the same event MUST always route to the same partition.

This preserves local ordering and prevents cross-partition orderbook matching.

---

# 24. P2.2 Partition Router

Create:

```text
internal/partition/
```

Responsibilities:

```go
type Router interface {
    Partition(eventID string) int
    Stream(eventID string) string
}
```

Hash function MUST remain stable across process restarts and deployments.

Do not use a random hash seed.

---

# 25. P2.3 Engine Partition Ownership

Each engine instance competes for partition ownership.

Lease:

```text
engine:lease:{partitionId}
```

Acquire:

```text
SET key instanceID NX PX ttl
```

Only the lease holder may process commands for that partition.

---

# 26. P2.4 Lease Renewal

Create a heartbeat loop:

```text
acquire
   ↓
renew periodically
   ↓
verify ownership
```

The renewal operation MUST verify that the owner is still the same instance.

Do not blindly run:

```text
SET key instanceID
```

because another instance may have acquired the lease.

Use a compare-and-renew Lua script or equivalent atomic operation.

---

# 27. P2.5 Fencing Tokens

Every lease acquisition generates an increasing fencing token:

```text
partition 3
token 18
```

Downstream writes contain:

```json
{
  "partitionId": 3,
  "fencingToken": 18
}
```

DB/settlement operations MUST reject writes from an older token.

This protects against a stale engine continuing to publish after its lease expires.

---

# 28. P2.6 Failover

Scenario:

```text
Engine A owns partition 3
Engine A crashes
      ↓
lease expires
      ↓
Engine B acquires partition 3
      ↓
Engine B obtains newer fencing token
      ↓
Engine B rebuilds state
      ↓
Engine B starts processing
```

No two active owners may safely process the same partition.

---

# 29. P2.7 State Reconstruction

New owner must:

```text
load commands:partition:3
 ↓
replay from sequence 0
 ↓
reconstruct orderbooks
 ↓
restore active orders
 ↓
resume from latest sequence
```

Optimization later:

```text
snapshot + command log tail
```

Do not introduce snapshots before deterministic replay works.

---

# 30. P2.8 Sequence Numbers

Every partition command MUST have a monotonically increasing sequence:

```text
partition 2:
1001
1002
1003
...
```

Every emitted event also receives a sequence.

Consumers can use:

```text
partitionId + sequence
```

for ordering and deduplication.

---

# 31. P2 Acceptance Criteria

Distributed Phase 1 is complete when:

- two engines can run simultaneously
- each partition has exactly one active owner
- commands for an event always reach the same partition
- killing an engine causes another engine to acquire its partitions
- the new engine rebuilds state
- stale engine writes are rejected
- commands are processed in partition order
- no duplicate trade execution occurs during failover
- DB and WS consumers receive ordered events

---

# 32. P3 — Production Readiness

## P3.1 Tests

Add tests for:

### Engine

```text
create event
create order
cancel order
limit matching
market matching
partial fill
FIFO
maker price
self trade prevention
duplicate order
replay
```

### Settlement

```text
trade insertion
balance debit
balance credit
order fill
event volume
transaction rollback
duplicate trade
```

### WebSocket

```text
valid envelope
trade event
unknown event
malformed payload
```

---

# 33. P3.2 Integration Tests

Use Testcontainers for:

```text
Postgres
Redis
```

Required scenario:

```text
API
 ↓
Redis
 ↓
Engine
 ↓
DB Worker
 ↓
Postgres
```

and:

```text
Engine
 ↓
Redis
 ↓
WS
```

The review currently reports no `*_test.go` coverage, so this should be treated as a required reliability milestone.

---

# 34. P3.3 Observability

Introduce structured logging.

Every command should carry:

```text
requestId
commandId
orderId
eventId
partitionId
sequence
instanceId
```

Example:

```json
{
  "level": "info",
  "event": "trade_executed",
  "eventId": "event-123",
  "orderId": "order-1",
  "tradeId": "trade-9",
  "partitionId": 3,
  "sequence": 1842
}
```

---

# 35. P3.4 Metrics

Expose:

```text
/orders/accepted
/orders/rejected
/orders/canceled
/orders/filled
/trades/executed
/settlement/failures
/redis/latency
/order/matching_latency
/event/replay_latency
/partition/lease_acquire
/partition/failover
```

Expose Prometheus endpoint.

The review notes that metrics exist partially but the `/metrics` endpoint and some observations are missing.

---

# 36. P3.5 Health Endpoints

Every service should expose:

```text
/health/live
/health/ready
```

Readiness must verify required dependencies.

Example:

```text
API:
  Redis reachable
  Postgres reachable

Engine:
  Redis reachable
  partition ownership available

DB Worker:
  Redis reachable
  Postgres reachable

WS:
  Redis reachable
```

---

# 37. P3.6 Graceful Shutdown

All services MUST:

1. stop accepting new work
2. stop consuming new messages
3. finish in-flight operations
4. flush durable state
5. close dependencies
6. exit

Shutdown must have a deadline.

---

# 38. P3.7 Configuration

Move all runtime settings to configuration/env:

```text
API_PORT
WS_PORT
DATABASE_URL
REDIS_URL
ENGINE_ID
PARTITION_COUNT
LEASE_TTL
LEASE_RENEW_INTERVAL
COMMAND_STREAM_PREFIX
DB_STREAM
WS_STREAM
```

No hardcoded production credentials.

---

# 39. P3.8 WebSocket Authentication

Add JWT authentication.

Connection flow:

```text
HTTP upgrade
 ↓
validate JWT
 ↓
derive user identity
 ↓
authorize subscriptions
 ↓
upgrade connection
```

Do not leave unrestricted origin validation in production.

---

# 40. P3.9 Redis RPC Reliability

Current `SendAndAwait` uses a fixed timeout and may leak goroutines on timeout.

Implement:

```text
requestId
timeout
cancellation
response cleanup
bounded retry
```

Responses must be correlated using:

```text
requestId
```

Do not rely on temporary channels that cannot be cleaned up.

---

# 41. Recommended Repository Structure

Target structure:

```text
internal/
  api/
  engine/
    engine.go
    matching.go
    recovery.go
    partition.go
    ownership.go
    replay.go
  events/
    envelope.go
    types.go
  partition/
    router.go
    lease.go
    fencing.go
  settlement/
    settlement.go
  dbworker/
    worker.go
  websocket/
    server.go
    subscriber.go
  outbox/
    outbox.go
  repository/
  observability/
    metrics.go
    logging.go

pkg/
  redis/
  protocol/
```

---

# 42. Implementation Order

Do not work on these in arbitrary order.

## Batch A — Event Contract

1. Add `EventEnvelope`
2. Define event types
3. Add sequence metadata
4. Update DB Worker decoder
5. Update WebSocket decoder

## Batch B — Trade Pipeline

1. Engine creates envelope
2. Push trade event to `db_processor`
3. DB Worker settles trade
4. WebSocket receives same envelope
5. Add integration test

## Batch C — DB Correctness

1. Fix SQL queries
2. Regenerate SQLC
3. Implement settlement transaction
4. Add idempotency
5. Fix order status consistency

## Batch D — Order Lifecycle

1. Client-generated order IDs
2. Persist orders
3. Cancellation
4. Correct orderbook endpoint
5. Deterministic order response

## Batch E — Engine Reliability

1. Fix shutdown
2. WAL `Sync`
3. Replay
4. Durable outbox
5. Publish retries

## Batch F — Matching Correctness

1. Self-trade prevention
2. Balance validation
3. Position validation
4. Fixed-point/decimal values

## Batch G — Distributed Phase 1

1. Partition router
2. Partition streams
3. Lease acquisition
4. Lease renewal
5. Fencing tokens
6. Failover
7. Replay

## Batch H — Production Readiness

1. Unit tests
2. Integration tests
3. Structured logs
4. Metrics
5. Health endpoints
6. Graceful shutdown
7. Configuration cleanup
8. WebSocket auth
9. Redis RPC cleanup

---

# 43. Definition of Done

The project is considered complete only when the following flow is reliable:

```text
Client
  ↓
API
  ↓
Command
  ↓
Correct partition
  ↓
Engine owner
  ↓
Orderbook
  ↓
Matching
  ↓
Durable event
  ├───────────────┐
  ▼               ▼
DB settlement     WebSocket
  ↓
Postgres
```

And after an engine crash:

```text
old engine dies
      ↓
lease expires
      ↓
new engine acquires partition
      ↓
replays command log
      ↓
reconstructs orderbook
      ↓
continues processing
```

The system MUST preserve:

```text
ordering
idempotency
durability
single ownership
deterministic matching
atomic settlement
```

---

# 44. Final Priority

The review's recommended priority should remain the implementation order:

```text
P0  Single-node pipeline
    ↓
P1  Correctness + durability
    ↓
P2  Partitioning + leases + failover
    ↓
P3  Production readiness
```

The critical architectural rule is:

> Do not scale the current shared `messages` queue horizontally.

First establish a durable, idempotent single-node command/event pipeline. Then introduce partition ownership on top of that command log.

This matches the review's conclusion that the matching core is a reasonable starting point, while the service integration and distributed layer remain the primary gaps.