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
- `GET /health/ready` — readiness that verifies dependencies and consumers
  (API: Redis + Postgres; DB-worker: Redis + Postgres + consumer progress;
  Engine: Redis + recovered partition ownership;
  WS: Redis)

These endpoints use a separate listener at `OPS_HOST:OPS_PORT`, defaulting to
`127.0.0.1:9090`. They are not mounted on the public API listener. For Kubernetes
HTTP probes, explicitly set `OPS_HOST=0.0.0.0` and restrict the ops port to cluster
probes and monitoring with network policy; do not publish it through ingress.

## Prerequisites

- Go 1.25+
- PostgreSQL 13+ (or 16+ for `gen_random_uuid()` built-in)
- Redis 6.2+ (`XAUTOCLAIM`, Streams + consumer groups)
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
# For an existing database that has migrations 001-003:
go run ./scripts/migrate -apply

# 3. Configure
export JWT_SECRET="$(openssl rand -hex 32)" # required by api + ws; at least 32 bytes
export DATABASE_URL="postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable"
export REDIS_URL="localhost:6379"
export ENGINE_ID="engine-0"
export PARTITION_COUNT="1"
# optional: LEASE_TTL=15s LEASE_RENEW_INTERVAL=5s COMMAND_STREAM_PREFIX=commands ...
# optional: OPS_HOST=127.0.0.1 WS_PORT=8080 API_PORT=3000
# Browser frontend on a different origin:
# export WS_ALLOWED_ORIGINS="http://localhost:3000"

# 4. Run the services (each in its own terminal)
OPS_PORT=9091 go run ./cmd/engine
OPS_PORT=9092 go run ./cmd/db-worker
OPS_PORT=9090 go run ./cmd/api
OPS_PORT=9093 go run ./cmd/ws
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

## API

Public:

- `POST /auth/signup`, `POST /auth/signin` → JWT

Authenticated (`Authorization: Bearer <token>`):

- `GET /me`, `GET /balances`, `GET /position`
- `GET /events`, `GET /event/:id`, `POST /event`
- `POST /order`, `DELETE /order/:orderId`
- `GET /orderbook/:eventId`, `GET /orderbook/:eventId/depth`

`/balances` returns the authenticated user's settled cash as
`data: {userId, balance}`. `/position` returns an array of that user's positions
with `eventId`, `outcome`, `shares`, `avgPrice`, and `updatedAt`. These endpoints
read the durable PostgreSQL projection and may briefly lag command processing;
the balance is settled cash, not the engine's available/reserved split.
Event responses consistently use the API fields `id`, `isActive`, and `expiresAt`.
Expired events are excluded from `/events`, and cannot accept new orders.
Order IDs must be UUIDs, and prices must be within the prediction market's 0–1 range.

Local service startup loads `.env` when present; explicit environment values
take precedence. `go run ./scripts/migrate` checks migration 004 without writing.
`go run ./scripts/migrate -apply` upgrades an existing 001–003 schema atomically,
backfills fills/remaining quantity and volume, and preserves existing row counts.

### WebSocket

Native clients connect to `ws://localhost:8080/ws` with
`Authorization: Bearer <jwt>`. Browser clients use
`new WebSocket("ws://localhost:8080/ws", ["bearer", jwt])`; the server negotiates
only `bearer`, never echoing the credential. Configure `WS_ALLOWED_ORIGINS` with
the exact frontend origin when it differs from the WebSocket server. With no
allowlist, only same-origin browser requests are accepted. Origin-free native
clients still require authentication. Use `wss://` when exposed.

URL tokens are disabled by default. Legacy clients can opt in with
`WS_ALLOW_QUERY_TOKEN=true`; application requests have `token` removed from their
URL, but reverse proxies must also redact query strings from access logs.

The server validates the JWT before upgrading and broadcasts
`events.EventEnvelope` payloads (e.g.
`order_created`, `trade_executed`, `depth_updated`) for subscribed events.

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
