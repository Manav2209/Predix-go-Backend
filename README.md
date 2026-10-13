# Predix — Distributed Prediction Market Backend

A Go backend for a **real-time prediction market exchange**, built around an in-memory order-matching engine, durable Redis command streams, PostgreSQL persistence, and WebSocket market updates.

Predix lets users create prediction events, place **YES/NO** limit or market orders, inspect order-book depth, and receive live market activity. The project explores the engineering behind an exchange: deterministic matching, balance and position reservations, event replay, partition ownership, asynchronous persistence, and operational visibility.

> **Project status:** An engineering/learning project, not a production trading platform. The repository contains the backend services; it does not include a frontend or a ready-to-run Docker Compose setup.

## Highlights

- **Price-time-priority matching:** Separate YES and NO order books with bids, asks, partial fills, and order cancellation.
- **Order types:** `LIMIT` and `MARKET` orders, with `BUY` and `SELL` sides.
- **Balance and position accounting:** Tracks available and reserved holdings to prevent an order from spending the same funds or selling the same shares twice.
- **Fixed-point pricing:** Prices are represented internally as integers with four decimal places (`1.00` → `10000`).
- **Redis Streams:** Commands reach the matching engine through partitioned durable streams; engine events feed a database worker.
- **Distributed engine foundations:** Event-based partition routing, Redis-backed leases, fencing, and replay on recovery.
- **Real-time updates:** JWT-authenticated WebSocket connections with event-specific subscriptions.
- **Persistence:** PostgreSQL with `pgx` and generated `sqlc` query code for users, events, orders, trades, positions, and transactions.
- **Operations:** Health/readiness endpoints, Prometheus metrics, structured engine logs, and graceful shutdown.
- **Tests:** Unit tests for matching, ledger, replay, partitioning, Redis, WebSocket behavior, plus integration tests.

## Tech stack

| Area | Technology |
| --- | --- |
| Language | Go 1.25.3 |
| HTTP API | Gin |
| WebSockets | Gorilla WebSocket |
| Messaging and coordination | Redis / Redis Streams |
| Database | PostgreSQL (`pgx/v5`) |
| Query generation | `sqlc` |
| Authentication | JWT, bcrypt |
| Observability | Prometheus, Go `slog` |
| Testing | Go testing, `miniredis`, Testcontainers |

## Architecture

```text
                     +-----------------------+
                     |       Clients         |
                     | REST / WebSocket      |
                     +-----------+-----------+
                                 |
                   +-------------+--------------+
                   |                            |
          +--------v--------+          +--------v---------+
          | Gin HTTP API    |          | WebSocket Gateway|
          | Auth / events / |          | Event subscribers|
          | orders / depth  |          +--------^---------+
          +-----+-------+---+                   |
                |       |                Redis updates
                |       v                       ^
                |   PostgreSQL                  |
                v                               |
       Redis command streams                    |
       (partitioned by event)                   |
                |                               |
         +------v-------------------------------+----+
         |          Matching Engine                  |
         | Order books / reservations / matching    |
         | Partition leases / command replay        |
         +---------------------+---------------------+
                               |
                         Redis event stream
                               |
                        +------v-------+
                        | DB Worker    |
                        | Projections  |
                        +------+-------+
                               |
                               v
                           PostgreSQL
```

**Order lifecycle:**

1. An authenticated client submits an order to the API.
2. The API validates the request and routes a command to the event's Redis partition.
3. The engine consumes the command, checks available balances or positions, and matches against resting orders using price-time priority.
4. The engine updates its in-memory order book and ledger, then emits persistence and live-update events.
5. The DB worker applies events to PostgreSQL; the WebSocket gateway distributes market updates to subscribed clients.

The engine can reconstruct in-memory state by replaying a partition's command log after a restart. This is a **replay-based design**, not a claim of fully validated exactly-once delivery or production-grade settlement.

## Repository structure

```text
cmd/
  api/               # Gin API process
  engine/            # Matching-engine process
  ws/                # WebSocket gateway
  db-worker/         # Redis-to-PostgreSQL persistence worker
internal/
  engine/            # Order books, matching, ledger, outbox, replay
  partition/         # Partition routing, leases, fencing
  dbworker/          # Async database projections
  handler/           # API handlers
  router/            # HTTP routes
  middleware/        # JWT middleware
  websocket/         # Hub, clients, Redis subscriber
  observability/     # Health checks and Prometheus metrics
  repository/        # sqlc-generated and custom database queries
  dto/               # API request/response types
  integration/       # Integration tests
  events/            # Event definitions
pkg/
  auth/              # JWT helpers
  config/            # Environment-based configuration
  redis/             # Redis manager and command messaging
  logging/           # Structured logging
db/
  migrations/        # Database schema migrations
  queries/           # SQL source queries for sqlc
spec/                # Reliability/distributed-engine design spec
sqlc.yaml            # sqlc configuration
scripts/
  migrate/           # Check/apply the existing-schema lifecycle migration
  smoke/             # Live API, WebSocket, persistence, and recovery checks
```

## Getting started

### Prerequisites

- Go **1.25.3** (as specified in `go.mod`)
- An existing PostgreSQL database and its connection string
- Redis 6.2+ (`XAUTOCLAIM` support); Redis 7 for the deployment lab
- Docker for integration tests and the optional disposable smoke-test database
- `psql` for initializing a fresh database from the SQL migrations

### 1. Clone and install dependencies

```bash
git clone https://github.com/Manav2209/Predix-go-Backend.git
cd Predix-go-Backend
go mod download
```

### 2. Use the existing PostgreSQL and Redis services

Keep PostgreSQL running as it is and supply its current connection string through
`DATABASE_URL`. The local and AWS deployment lab uses this existing database;
it does not require a new PostgreSQL container, RDS instance, or database EBS volume.
Preserve the connection's required TLS options.

Reuse Redis if it is already running. For a fresh local Redis instance, the
following example enables AOF and a persistent volume with a loopback-only port:

```bash
docker run -d --name predix-redis \
  -p 127.0.0.1:6379:6379 -v predix-redis-data:/data \
  redis:7-alpine redis-server --appendonly yes
```

### 3. Configure environment

Local service startup automatically loads `.env` from the working directory.
Explicit environment variables take precedence. The `.env` file is ignored by Git.

```dotenv
DATABASE_URL=<your-existing-postgresql-connection-string>
REDIS_URL=localhost:6379
JWT_SECRET=<your-random-secret-of-at-least-32-bytes>
API_PORT=3000
WS_PORT=8080
PARTITION_COUNT=1
```

Generate a random secret with `openssl rand -hex 32` and save it in `.env`.
Use the **same** JWT secret, Redis settings, stream names, and partition count
across the relevant services. `ENGINE_ID` defaults to a generated UUID; if
overridden, it must identify a unique engine instance.

### 4. Check or apply database schema

For an existing database with migrations 001–003, the dedicated command checks
migration 004 by default and applies it only with the explicit flag:

```bash
go run ./scripts/migrate
go run ./scripts/migrate -apply
```

Migration 004 runs atomically through this command, backfills fills, remaining
quantity and event volume, and verifies existing row counts before committing.
If the required lifecycle columns already exist, the command reports success
without reapplying the migration.

For a **fresh** database, export the same `DATABASE_URL` configured in `.env`
and apply the `*.up.sql` files in numerical order:

```bash
for migration in db/migrations/*.up.sql; do
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration" || exit 1
done
```

Some rollback filenames use different naming conventions; review them before
configuring an automated migration tool. Do not rerun the initial migrations
against an already initialized database.

### 5. Run all four services

Open four terminals in the repository root (with the environment variables loaded in each):

```bash
# Terminal 1 — HTTP API
OPS_PORT=9090 go run ./cmd/api

# Terminal 2 — Matching engine
OPS_PORT=9091 go run ./cmd/engine

# Terminal 3 — Database worker
OPS_PORT=9092 go run ./cmd/db-worker

# Terminal 4 — WebSocket gateway
OPS_PORT=9093 go run ./cmd/ws
```

All four services use a separate operations listener. The example uses ops ports
`9090` for API, `9091` for engine, `9092` for DB worker, and `9093` for WebSockets.
On PowerShell, set `$env:OPS_PORT` to the corresponding value before `go run`.

Test the API:

```bash
curl http://localhost:3000/ping
# pong

curl http://localhost:9090/health/ready
```

## Configuration

| Variable              | Default                          | Description                            |
|-----------------------|----------------------------------|----------------------------------------|
| `API_PORT`            | `PORT` \|\| `3000`               | REST API port                          |
| `WS_PORT`             | `8080`                           | WebSocket port                         |
| `OPS_PORT`            | `9090`                           | metrics/health port                    |
| `OPS_HOST`            | `127.0.0.1`                      | private metrics/health bind address    |
| `DATABASE_URL`        | *(required for api/db-worker)*   | explicit Postgres DSN; no credentials fallback |
| `REDIS_URL`           | *(required)*                     | Redis host:port address                |
| `JWT_SECRET`          | *(required for api/ws)*          | random HS256 secret, at least 32 bytes |
| `WS_ALLOWED_ORIGINS`  | *(same origin only)*             | comma-separated exact browser origins |
| `WS_ALLOW_QUERY_TOKEN`| `false`                          | opt in to legacy URL tokens           |
| `ENGINE_ID`           | generated UUID                   | unique engine instance id (lease owner)|
| `PARTITION_COUNT`     | `1`                              | number of engine partitions            |
| `LEASE_TTL`           | `15s`                            | partition lease duration               |
| `LEASE_RENEW_INTERVAL`| `5s`                             | lease renew cadence                    |
| `COMMAND_STREAM_PREFIX` | `commands`                     | prefix for `commands:{p}` streams      |
| `DB_STREAM`           | `events:out`                     | event stream consumed by DB worker     |
| `DB_GROUP`            | `db-workers`                     | DB worker consumer group               |
| `WS_STREAM`           | `ws:updates`                     | WS fan-out channel                     |

## HTTP API

The base URL is `http://localhost:3000`. Unless explicitly stated, endpoints require an `Authorization: Bearer <JWT>` header.

| Method | Route | Description | Auth |
| --- | --- | --- | --- |
| `POST` | `/auth/signup` | Register a user | Public |
| `POST` | `/auth/signin` | Get a JWT | Public |
| `GET` | `/me` | Get current user ID | JWT |
| `GET` | `/balances` | Get account balances | JWT |
| `GET` | `/position` | Get positions | JWT |
| `GET` | `/events` | List active events | JWT |
| `GET` | `/event/:id` | Get an event | JWT |
| `POST` | `/event` | Create an event | JWT |
| `POST` | `/order` | Place an order | JWT |
| `DELETE` | `/order/:orderId` | Cancel an order | JWT |
| `GET` | `/orderbook/:eventId` | Get order-book depth | JWT |
| `GET` | `/orderbook/:eventId/depth` | Get order-book depth | JWT |
| `GET` | `/ping` | Basic ping | Public |
| `GET` | `/` | Basic service greeting | Public |

Health and metrics endpoints use the separate operations ports described above.
They are not public API routes.

`/balances` returns the authenticated user's settled cash as
`data: {userId, balance}`. `/position` returns an array of that user's positions
with `eventId`, `outcome`, `shares`, `avgPrice`, and `updatedAt`. These read the
durable PostgreSQL projection and may briefly lag command processing. The
balance is settled cash, not the engine's available/reserved split.

Event responses consistently use `id`, `isActive`, and `expiresAt`. Expired
events are excluded from `/events`, and cannot accept new orders. Event creation
requires an `expiresAt` value in the future.

### Sign up / sign in

```bash
curl -X POST http://localhost:3000/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"change-me-123"}'

curl -X POST http://localhost:3000/auth/signin \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"change-me-123"}'
```

The sign-in response includes `token` and a `user` object. Save that token:

```bash
export TOKEN='<token-from-signin-response>'
```

### Create a prediction event

```bash
curl -X POST http://localhost:3000/event \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Prediction market example",
    "description": "An example market for testing the engine",
    "question": "Will the event happen?",
    "imageurl": "https://example.com/market.png",
    "expiresAt": "2027-12-31T23:59:59Z"
  }'
```

Note: the request field is currently named **`imageurl`** (all lowercase), as defined in the DTO.

### Place a limit order

```bash
curl -X POST http://localhost:3000/order \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "<event-uuid>",
    "orderType": "LIMIT",
    "outcome": "YES",
    "side": "BUY",
    "quantity": 10,
    "price": 0.45
  }'
```

`orderType`: `LIMIT` or `MARKET`; `outcome`: `YES` or `NO`; `side`: `BUY` or `SELL`.
Limit prices must be greater than zero and no higher than `1.0`. Optional
`orderId` values must be UUIDs; the API generates one when omitted. Order
responses expose the resulting engine state; internal prices use fixed-point
integers in units of `1/10000`.

### View depth and cancel

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/orderbook/<event-uuid>/depth

curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/order/<order-uuid>
```

## Real-time WebSocket updates

The WebSocket gateway listens at `ws://localhost:8080/ws`. Native clients send
`Authorization: Bearer <JWT>`; browser clients authenticate through the
`bearer` subprotocol:

```javascript
const socket = new WebSocket("ws://localhost:8080/ws", ["bearer", jwt]);
```

The server negotiates only `bearer`, never echoing the token. Set
`WS_ALLOWED_ORIGINS` to exact, comma-separated browser origins when the frontend
origin differs from the gateway. With no allowlist, browser connections must be
same-origin. Origin-free native clients still require authentication. Use `wss://`
for exposed connections.

Query-string JWTs are disabled by default. Legacy clients require explicit
`WS_ALLOW_QUERY_TOKEN=true`; application requests have `token` removed from their
URL, but upstream proxies must also redact URL credentials from access logs.

Subscribe to one or more event IDs:

```json
{"method":"SUBSCRIBE","params":["<event-uuid>"]}
```

Unsubscribe:

```json
{"method":"UNSUBSCRIBE","params":["<event-uuid>"]}
```

The gateway delivers `depth` messages with YES/NO books and `trade` messages with
trade details to subscribed clients. It also implements heartbeat ping/pong
handling. Engine book changes, including cancellations, publish depth updates;
Redis event envelopes are the internal transport rather than the browser wire format.

## Matching and reliability model

### Matching

Each prediction event has separate order books for its **YES** and **NO** outcomes. Bids are ordered highest price first; asks lowest price first; orders at the same price follow FIFO. Both full and partial fills are modeled, with order states such as `PENDING`, `PARTIAL`, `FILLED`, `CANCELED`, and `REJECTED`.

### Ledger

The engine maintains available and reserved cash as well as available and reserved shares. Buy orders reserve funds and sell orders reserve shares; fills and cancellations adjust those reservations. A new user is granted a **development starting balance of 100.00** in the engine ledger. This is simulated money, not real-money settlement.

### Distribution and recovery

Events map to command-stream partitions. The engine uses partition leases and fencing to coordinate ownership, while the command log provides a replay path to reconstruct in-memory state after restart. Engine outputs are asynchronously projected to PostgreSQL by the DB worker, and market updates are fanned out through Redis to WebSocket subscribers.

These mechanisms are the focus of the project; do not assume they have been certified for financial production use or subjected to published performance benchmarks.

## Reliability notes

- **Lease/fencing**: partition ownership is a Redis lease; acquiring it bumps
  a fencing token stamped on every event. Stale writers are dropped by the
  DB worker.
- **RPC**: `SendAndAwait` correlates replies by `requestId`, has a bounded
  deadline, and retries durable appends; timeouts do not leak goroutines or
  lose commands.
- **Shutdown**: every service drains in-flight work with a deadline.
- **DB consumer recovery**: pending events are reclaimed every five seconds
  once idle for ten seconds, scanning up to ten pages of 100 entries per pass.
  The scan cursor carries over between passes. Operational processing failures
  receive three attempts with backoff, then stop the process with a nonzero
  exit while leaving the event pending. Readiness fails before startup, after
  termination, or after 30 seconds without consumer progress; idle successful
  reads count as progress.
- **Dead letters**: malformed envelopes/payloads are preserved in
  `<DB_STREAM>:dead-letter` with their source stream/id, original entry, and
  error before the source is acknowledged. Monitor this stream and repair and
  requeue entries after investigating them. Database/Redis failures are not
  dead-lettered. A failed dead-letter write leaves the source pending.
- **Replay recovery**: a partition cannot consume or advertise recovered
  ownership until replay succeeds. Corrupt envelopes, unsupported commands,
  and unreadable payloads fail recovery. The engine releases the lease and
  retries with exponential backoff capped at 30 seconds; repair corrupt log
  entries before the partition can recover. Valid business rejections in the
  command log are reproduced by the command handlers.
- **Metrics**: `engine_orders_accepted_total`, `engine_trades_executed_total`,
  `engine_settlement_failures_total`, `engine_redis_latency_seconds`,
  `engine_matching_latency_seconds`, `engine_replay_latency_seconds`,
  `engine_partition_owner`, `engine_lease_losses_total`, and more.
- **Logging**: JSON structured logs carrying `requestId`, `commandId`,
  `orderId`, `eventId`, `partitionId`, `sequence`, and `instanceId`.
- Prices are fixed-point integers (`1/10000`); balances/shares are int64.

## Observability

Every service exposes `/health/live`, `/health/ready`, and `/metrics` through a
separate `OPS_HOST:OPS_PORT` listener. The default bind address is
`127.0.0.1:9090`; operational endpoints are not mounted on the public API port.
Use a different ops port for each service when running them on one machine.

Readiness checks Redis and, where applicable, PostgreSQL. The engine also
requires recovered partition ownership; the DB worker checks consumer progress.
For Kubernetes HTTP probes, set `OPS_HOST=0.0.0.0` and restrict the ops port to
probes and monitoring through network policy. Keep it out of public ingress.

Prometheus exposes process/runtime metrics on all services and matching-engine
metrics on the engine process. Engine logs include request, command, order,
event, partition, and instance identifiers where applicable.

## Tests

```bash
go test ./...

# Engine stress (repeat run to surface flakes)
go test ./internal/engine -count=2

# Integration tests (need Docker) — full API→Redis→Engine→DB Worker→Postgres
# flow plus Engine→Redis→WS; skips automatically when Docker is unavailable.
go test ./internal/integration -v

# Live HTTP + WebSocket smoke test of all four service binaries.
# Uses the configured Redis, disposable local PostgreSQL, isolated test data,
# and a generated JWT secret. Stops test services and removes test data afterward.
# Requires Docker, and no other engine holding Redis partition 0.
mkdir -p tmp/smoke/bin
go build -o tmp/smoke/bin/ ./cmd/api ./cmd/engine ./cmd/db-worker ./cmd/ws
go run ./scripts/smoke

# Read-only connectivity check against .env dependencies:
go run ./scripts/smoke -probe
```

The disposable PostgreSQL container is a test fixture; normal application
startup continues to use the existing `DATABASE_URL`. For query-source changes,
regenerate generated database access code with `sqlc generate`.

## Known development limitations

- No bundled Compose/Kubernetes deployment configuration or frontend.
- `JWT_SECRET` must be explicitly supplied and contain at least 32 bytes.
- Redis TLS and authentication configuration must be added before the planned managed-Redis deployment.
- There is no public share-issuance API to bootstrap a successful SELL from a new
  account. Current settlement tests use isolated fixtures; genuine matching load
  tests need a controlled way to seed holdings.
- User creation provides simulated starting funds for development.
- Replay currently scans the command history; snapshotting/compaction may be useful as history grows.
- This is not a production-ready real-money trading or settlement system.

## License

Licensed under the [MIT License](LICENSE).

Built by [Manav Panchal](https://github.com/Manav2209).
