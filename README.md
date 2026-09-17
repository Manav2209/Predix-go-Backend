# predix

A distributed prediction-market backend: a partition-sharded order-matching
engine with Redis as the durable command log, event sourcing + replay-based
state reconstruction, a Postgres projection worker, and real-time WebSocket
fan-out.

## Architecture

```
                    ┌─────────────┐
   HTTP API ───────►│  cmd/api    │   REST / JSON (Gin)
                    └──────┬──────┘
                           │  commands (request/response via Redis pub/sub)
                           ▼
                    ┌─────────────┐    partition stream
                    │  cmd/engine │◄──│  commands:{p}    (Redis Streams)
                    └──────┬──────┘    leases, fencing tokens
                           │  events:{out} (durable event stream)
                           ▼
                    ┌─────────────┐
                    │ cmd/db-worker│──► Postgres projection
                    └─────────────┘
                           │  ws:updates (pub/sub)
                           ▼
                    ┌─────────────┐
                    │   cmd/ws    │──► WebSocket clients
                    └─────────────┘
              every service: /metrics + /health/* on the OPS port
```

- **Commands are the source of truth.** Every state change is appended to a
  per-partition Redis Stream (`commands:{p}`). On startup (or failover) an
  engine replays its partition's stream to reconstruct the exact order book
  and ledger.
- **Fencing tokens** protect against stale writers: each lease acquisition
  increments a fence counter; the forwarder and DB worker drop any write
  stamped with an older token.
- **Idempotent handlers** (duplicate trades, replayed commands) make
  at-least-once delivery safe.

## Services (`cmd/*`)

| Binary    | Role                                          | Port  |
|-----------|-----------------------------------------------|-------|
| `api`     | REST API, auth, routes commands to the engine | `API_PORT` (3000) |
| `engine`  | Match engine, partition leases, events emit   | `OPS_PORT` (9090) |
| `db-worker` | Consumes events, projects them into Postgres | `OPS_PORT` (9090) |
| `ws`      | WebSocket fan-out (JWT-protected upgrade)     | `WS_PORT` (8080) |

Every service exposes:

- `GET /metrics` — Prometheus endpoint
- `GET /health/live` — liveness
- `GET /health/ready` — readiness that verifies dependencies
  (API/DB-worker: Redis + Postgres; Engine: Redis + partition ownership;
  WS: Redis)

## Prerequisites

- Go 1.25+
- PostgreSQL 13+ (or 16+ for `gen_random_uuid()` built-in)
- Redis 6+ (Streams + consumer groups)
- Docker (only for the integration tests)

## Quickstart

```bash
# 1. Start dependencies (adjust to taste)
docker run -d --rm -p 5432:5432 -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=predix postgres:16-alpine
docker run -d --rm -p 6379:6379 redis:7-alpine

# 2. Apply migrations
#   (run the *.up.sql files in db/migrations in order, e.g. with psql)
psql "postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable" \
  -f db/migrations/001_create_users.up.sql
# ...repeat for 002, 003, 004...

# 3. Configure
export JWT_SECRET="change-me"          # required; api + ws refuse to start without it
export DATABASE_URL="postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable"
export REDIS_URL="localhost:6379"
export ENGINE_ID="engine-0"
export PARTITION_COUNT="1"
# optional: LEASE_TTL=15s LEASE_RENEW_INTERVAL=5s COMMAND_STREAM_PREFIX=commands ...
# optional: OPS_PORT=9090 WS_PORT=8080 API_PORT=3000

# 4. Run the services (each in its own terminal)
go run ./cmd/engine
go run ./cmd/db-worker
go run ./cmd/api
go run ./cmd/ws
```

## Configuration

| Variable              | Default                          | Description                            |
|-----------------------|----------------------------------|----------------------------------------|
| `API_PORT`            | `PORT` \|\| `3000`               | REST API port                          |
| `WS_PORT`             | `8080`                           | WebSocket port                         |
| `OPS_PORT`            | `9090`                           | metrics/health port                    |
| `DATABASE_URL`        | `postgres://postgres:...`        | Postgres DSN                           |
| `REDIS_URL`           | `localhost:6379`                 | Redis address                          |
| `JWT_SECRET`          | *(required)*                     | HS256 secret for sign-in + WS auth     |
| `ENGINE_ID`           | `engine-0`                       | unique engine instance id (lease owner)|
| `PARTITION_COUNT`     | `1`                              | number of engine partitions            |
| `LEASE_TTL`           | `15s`                            | partition lease duration               |
| `LEASE_RENEW_INTERVAL`| `5s`                             | lease renew cadence                    |
| `COMMAND_STREAM_PREFIX` | `commands`                     | prefix for `commands:{p}` streams      |
| `DB_STREAM`           | `events:out`                     | event stream consumed by DB worker     |
| `DB_GROUP`            | `db-workers`                     | DB worker consumer group               |
| `WS_STREAM`           | `ws:updates`                     | WS fan-out channel                     |

## API

Public:

- `POST /auth/signup`, `POST /auth/signin` → JWT

Authenticated (`Authorization: Bearer <token>`):

- `GET /me`, `GET /balances`, `GET /position`
- `GET /events`, `GET /event/:id`, `POST /event`
- `POST /order`, `DELETE /order/:orderId`
- `GET /orderbook/:eventId`, `GET /orderbook/:eventId/depth`

### WebSocket

Connect to `ws://localhost:8080/ws?token=<jwt>`. The server validates the JWT
before upgrading and broadcasts `events.EventEnvelope` payloads (e.g.
`order_created`, `trade_executed`, `depth_updated`) for subscribed events.

## Reliability notes

- **Lease/fencing**: partition ownership is a Redis lease; acquiring it bumps
  a fencing token stamped on every event. Stale writers are dropped by the
  DB worker.
- **RPC**: `SendAndAwait` correlates replies by `requestId`, has a bounded
  deadline, and retries durable appends; timeouts do not leak goroutines or
  lose commands.
- **Shutdown**: every service drains in-flight work with a deadline.
- **Metrics**: `engine_orders_accepted_total`, `engine_trades_executed_total`,
  `engine_matching_latency_seconds`, `engine_replay_latency_seconds`,
  `engine_partition_owner`, `engine_lease_losses_total`, and more.
- **Logging**: JSON structured logs carrying `requestId`, `commandId`,
  `orderId`, `eventId`, `partitionId`, `sequence`, and `instanceId`.
- Prices are fixed-point integers (`1/10000`); balances/shares are int64.

## Tests

```bash
go test ./...

# Engine stress (repeat run to surface flakes)
go test ./internal/engine -count=2

# Integration tests (need Docker) — full API→Redis→Engine→DB Worker→Postgres
# flow plus Engine→Redis→WS; skips automatically when Docker is unavailable.
go test ./internal/integration -v
```

## Repository layout

```
cmd/api, cmd/engine, cmd/db-worker, cmd/ws   service entry points
internal/partition    router + leases + fencing tokens
internal/engine       matching core, replay, failover loop, outbox, metrics
internal/dbworker     Postgres projection worker (fenced)
internal/websocket    hub + Redis subscriber + JWT-protected upgrade
internal/handler      REST handlers and DTOs
internal/repository   sqlc-generated Postgres queries
internal/observability shared ops endpoints (health + Prometheus)
pkg/config            env-driven configuration
pkg/logging           JSON structured logger
pkg/redis             RedisManager, send/await RPC, requestId correlation
pkg/auth              JWT issue/validate
db/migrations         SQL migrations
db/queries            sqlc query source
spec/                 the implementation spec the code follows
```

The behavior follows `spec/Predix Reliability & Distributed Matching Engine —
Implementation Spec.md`.