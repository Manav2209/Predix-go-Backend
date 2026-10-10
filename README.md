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
  repository/        # sqlc-generated database access
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
```

## Getting started

### Prerequisites

- Go **1.25.3** (as specified in `go.mod`)
- PostgreSQL
- Redis
- A SQL migration runner or `psql` to apply the migration SQL

### 1. Clone and install dependencies

```bash
git clone https://github.com/Manav2209/Predix-go-Backend.git
cd Predix-go-Backend
go mod download
```

### 2. Start PostgreSQL and Redis

For a quick local setup using Docker:

```bash
docker run -d --name predix-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_DB=predix \
  -p 5432:5432 postgres:16

docker run -d --name predix-redis -p 6379:6379 redis:7
```

These are **development-only** credentials and exposed ports.

### 3. Apply database schema

From the project root, apply the `*.up.sql` files in numerical order:

```bash
for migration in db/migrations/*.up.sql; do
  psql "postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable" \
    -v ON_ERROR_STOP=1 -f "$migration" || exit 1
done
```

The repository contains SQL migration files, but no checked-in migration CLI configuration. Note that some rollback filenames do not follow the same naming convention as the `*.up.sql` files; review them before configuring an automated migration tool.

### 4. Configure environment

The application reads **environment variables**, not an `.env` file automatically. Export the following variables or load them using your preferred shell tooling:

```bash
export DATABASE_URL='postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable'
export REDIS_URL='localhost:6379'
export JWT_SECRET='replace-with-a-long-random-secret'

export API_PORT=3000
export WS_PORT=8080
export OPS_PORT=9090

export PARTITION_COUNT=1
export ENGINE_ID=engine-0
```

Use the **same** `JWT_SECRET`, Redis configuration, stream names, and partition count across relevant services.

### 5. Run all four services

Open four terminals in the repository root (with the environment variables loaded in each):

```bash
# Terminal 1 — HTTP API
go run ./cmd/api

# Terminal 2 — Matching engine
go run ./cmd/engine

# Terminal 3 — Database worker
OPS_PORT=9091 go run ./cmd/db-worker

# Terminal 4 — WebSocket gateway
OPS_PORT=9092 go run ./cmd/ws
```

**Important:** The API serves metrics and health checks on its API port. Engine, worker, and WebSocket processes run separate operations servers, so each needs its **own `OPS_PORT`** when running on the same machine. The example uses `9090` for the engine, `9091` for the DB worker, and `9092` for WebSockets.

Test the API:

```bash
curl http://localhost:3000/ping
# pong

curl http://localhost:3000/health/ready
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | Local PostgreSQL URL | Database connection string |
| `REDIS_URL` | `localhost:6379` | Redis connection |
| `JWT_SECRET` | *(empty)* | Secret for signing/verifying tokens; **set explicitly** |
| `API_PORT` | `3000` | HTTP API port (`PORT` is a fallback) |
| `WS_PORT` | `8080` | WebSocket gateway port |
| `OPS_PORT` | `9090` | Non-API health/metrics port |
| `DB_STREAM` | `events:out` | Persistence event stream |
| `DB_GROUP` | `db-workers` | Database-worker consumer group |
| `WS_STREAM` | `ws:updates` | Live-update Redis channel |
| `ENGINE_ID` | `engine-0` | Identifier for engine lease ownership |
| `PARTITION_COUNT` | `1` | Number of event partitions |
| `COMMAND_STREAM_PREFIX` | `commands` | Prefix for partition command streams |
| `LEASE_TTL` | `15s` | Partition lease duration |
| `LEASE_RENEW_INTERVAL` | `5s` | Lease renewal cadence |

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
| `GET` | `/health/live` | Liveness | Public |
| `GET` | `/health/ready` | Dependency readiness | Public |
| `GET` | `/metrics` | Prometheus metrics | Public |

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

`orderType`: `LIMIT` or `MARKET`; `outcome`: `YES` or `NO`; `side`: `BUY` or `SELL`. The `price` field is required for limit orders. Order responses expose the resulting engine data; internal prices use four-decimal fixed-point units.

### View depth and cancel

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/orderbook/<event-uuid>/depth

curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/order/<order-uuid>
```

## Real-time WebSocket updates

The WebSocket gateway listens at `ws://localhost:8080/ws` and requires a JWT in the `token` query parameter:

```text
ws://localhost:8080/ws?token=<JWT>
```

Subscribe to one or more event IDs:

```json
{"method":"SUBSCRIBE","params":["<event-uuid>"]}
```

Unsubscribe:

```json
{"method":"UNSUBSCRIBE","params":["<event-uuid>"]}
```

The gateway forwards market updates to subscribed clients and implements heartbeat ping/pong handling. Be aware that query-string tokens can be captured in logs and that the current WebSocket upgrader accepts all origins; this needs tightening before public deployment.

## Matching and reliability model

### Matching

Each prediction event has separate order books for its **YES** and **NO** outcomes. Bids are ordered highest price first; asks lowest price first; orders at the same price follow FIFO. Both full and partial fills are modeled, with order states such as `PENDING`, `PARTIAL`, `FILLED`, `CANCELED`, and `REJECTED`.

### Ledger

The engine maintains available and reserved cash as well as available and reserved shares. Buy orders reserve funds and sell orders reserve shares; fills and cancellations adjust those reservations. A new user is granted a **development starting balance of 100.00** in the engine ledger. This is simulated money, not real-money settlement.

### Distribution and recovery

Events map to command-stream partitions. The engine uses partition leases and fencing to coordinate ownership, while the command log provides a replay path to reconstruct in-memory state after restart. Engine outputs are asynchronously projected to PostgreSQL by the DB worker, and market updates are fanned out through Redis to WebSocket subscribers.

These mechanisms are the focus of the project; do not assume they have been certified for financial production use or subjected to published performance benchmarks.

## Observability

- `/health/live`: process liveness.
- `/health/ready`: readiness checks against Redis, PostgreSQL, and/or owned engine partitions, depending on the process.
- `/metrics`: Prometheus-compatible process/runtime metrics, plus matching-engine metrics on the engine process.

The API exposes these on `API_PORT`; the engine, DB worker, and WebSocket process expose them on their respective `OPS_PORT`.

## Running tests

```bash
# Unit and package tests
go test ./...

# Verbose tests
go test -v ./internal/engine ./internal/partition
```

Integration tests use Testcontainers and may require a running Docker daemon. To regenerate SQL query code after changing SQL sources, install `sqlc` and run:

```bash
sqlc generate
```

## Known development limitations

- No bundled Compose/Kubernetes deployment configuration or frontend.
- `JWT_SECRET` must be supplied; don't deploy with an empty secret.
- The WebSocket origin check currently permits all origins and JWTs are passed in URL query strings.
- User creation provides simulated starting funds for development.
- Replay currently scans the command history; snapshotting/compaction may be useful as history grows.
- This is not a production-ready real-money trading or settlement system.

## License

Licensed under the [MIT License](LICENSE).

Built by [Manav Panchal](https://github.com/Manav2209).
