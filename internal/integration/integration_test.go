// Package integration exercises the P3.2 required scenarios against real
// Postgres and Redis via Testcontainers:
//
//	API → Redis → Engine → DB Worker → Postgres
//	Engine → Redis → WS
//
// These tests require Docker and self-skip when a daemon is unavailable.
package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"predix/internal/dbworker"
	"predix/internal/engine"
	"predix/internal/events"
	"predix/internal/handler"
	"predix/internal/partition"
	"predix/internal/repository"
	"predix/internal/router"
	"predix/pkg/auth"
	pkgredis "predix/pkg/redis"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
)

const (
	itestStream = "itest:events"
	itestGroup  = "itest:db-workers"
	itestWS     = "itest:ws"
	itestCmdPre = "itest:cmds"
	itestSecret = "itest-secret"
	itestEngine = "itest-engine-1"
)

func TestP32Integration(t *testing.T) {
	if !dockerReady(t) {
		t.Skip("Docker is unavailable; integration test skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgAddr := startPostgres(t, ctx)
	redisAddr := startRedis(t, ctx)

	runMigrations(t, pgAddr)

	pool, err := pgxpool.New(ctx, postgresDSN(pgAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// ---- API layer (mirrors cmd/api) ----
	auth.Init(itestSecret)

	rm := pkgredis.NewRedisManager(redisAddr, "")
	defer rm.Close()

	rm.SetRouter(partition.NewRouter(1, itestCmdPre))

	queries := repository.New(pool)

	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	router.SetupRoutes(g, handler.NewHandler(queries, rm))

	var api *httptest.Server

	t.Cleanup(func() {
		if api != nil {
			api.Close()
		}
	})

	// ---- Engine layer (mirrors cmd/engine) ----
	eng, err := engine.NewEngineWithOutbox(rm, filepath.Join(t.TempDir(), "outbox.log"))
	if err != nil {
		t.Fatal(err)
	}

	eng.SetRouter(partition.NewRouter(1, itestCmdPre))
	eng.SetLeaseSettings(itestEngine, 2*time.Second, 400*time.Millisecond)
	eng.SetStreamNames(itestStream, itestWS)

	if err := eng.AddPartition(0); err != nil {
		t.Fatal(err)
	}

	eng.Start()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		eng.Shutdown(shutdownCtx)
	})

	// ---- DB Worker layer (mirrors cmd/db-worker) ----
	worker := dbworker.NewWithStreams(client, pool, queries, itestStream, itestGroup)

	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)

	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	// ---- WS fan-out probe (bottom of Engine → Redis → WS) ----
	wsSub := client.Subscribe(ctx, itestWS)
	t.Cleanup(func() { _ = wsSub.Close() })

	if _, err := wsSub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	wsMsgs := wsSub.Channel()

	waitFor(t, 15*time.Second, func() bool {
		return eng.OwnedPartitions() == 1
	}, "engine acquires the partition lease")

	api = httptest.NewServer(g)

	// ---- Scenario A: order lifecycle across every layer ----

	alice := signupAndSignin(t, api.URL, "alice@itest.dev")
	bob := signupAndSignin(t, api.URL, "bob@itest.dev")

	eventID := createEventViaAPI(t, api.URL, alice.token)

	// A resting BUY: the engine accepts it, the DB worker persists it, and
	// the WS fan-out emits order_created.
	orderID := uuid.NewString()

	status := createOrderViaAPI(t, api.URL, bob.token, map[string]any{
		"orderId":   orderID,
		"eventId":   eventID,
		"side":      "BUY",
		"outcome":   "YES",
		"orderType": "LIMIT",
		"quantity":  10,
		"price":     0.5,
	})
	if status != "PENDING" && status != "PARTIAL" {
		t.Fatalf("resting order status = %s, want PENDING/PARTIAL", status)
	}

	waitFor(t, 20*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM orders WHERE id = $1`, orderID).Scan(&n)
		return n == 1
	}, "db worker persists the order into Postgres")

	var dbSide, dbOutcome, dbType string
	if err := pool.QueryRow(ctx,
		`SELECT side, outcome, order_type FROM orders WHERE id = $1`, orderID,
	).Scan(&dbSide, &dbOutcome, &dbType); err != nil {
		t.Fatal(err)
	}
	if dbSide != "BUY" || dbOutcome != "YES" || dbType != "LIMIT" {
		t.Errorf("order row = (%s, %s, %s), want (BUY, YES, LIMIT)", dbSide, dbOutcome, dbType)
	}

	// ---- Scenario B: Engine → Redis → WS (order_created arrives) ----
	var wsOrder engine.Order

	if err := expectWSEvent(t, wsMsgs, events.EventOrderCreated, &wsOrder); err != nil {
		t.Error(err)
	}

	if wsOrder.ID != orderID {
		t.Errorf("ws order id = %s, want %s", wsOrder.ID, orderID)
	}

	// ---- Settlement semantics (P3.1) via the DB worker ----
	// The engine requires the seller to already hold shares; with no share
	// issuance step, a real cross-user trade cannot be seeded through the API.
	// So we feed the worker a trade the way the engine would after a match and
	// assert every settlement invariant.

	token, err := currentFencingToken(ctx, client, 0)
	if err != nil {
		t.Fatal(err)
	}

	tradeID := uuid.NewString()

	trade := &engine.Trade{
		ID:           tradeID,
		EventID:      eventID,
		OrderID:      orderID,
		MatchOrderID: orderID,
		BuyerID:      bob.id,
		SellerID:     alice.id,
		Outcome:      engine.OutcomeYes,
		TakerSide:    engine.SideBuy,
		Quantity:     10,
		Price:        5000,
	}

	if err := injectTrade(ctx, client, itestStream, trade, token); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 20*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM trades`).Scan(&n)
		return n == 1
	}, "db worker settles the trade")

	if wsErr := expectWSEvent(t, wsMsgs, events.EventTradeExecuted, nil); wsErr != nil {
		t.Error(wsErr)
	}

	var priceScaled, quantity int64
	if err := pool.QueryRow(ctx,
		`SELECT (price * 10000)::bigint, quantity::bigint FROM trades WHERE id = $1`, tradeID,
	).Scan(&priceScaled, &quantity); err != nil {
		t.Fatal(err)
	}
	if priceScaled != 5000 || quantity != 10 {
		t.Errorf("trade price/qty = %d/%d, want 5000/10", priceScaled, quantity)
	}

	var fill, dbStatus string
	if err := pool.QueryRow(ctx,
		`SELECT filled_quantity::bigint::text, status FROM orders WHERE id = $1`, orderID,
	).Scan(&fill, &dbStatus); err != nil {
		t.Fatal(err)
	}
	if fill != "10" || dbStatus != "FILLED" {
		t.Errorf("order fill/status = %s/%s, want 10/FILLED", fill, dbStatus)
	}

	// Buyer (bob) pays 5.0; seller (alice) receives 5.0 from a 100.0 base.
	assertBalance(t, ctx, pool, bob.id, 100*engine.PriceScale-50000)
	assertBalance(t, ctx, pool, alice.id, 100*engine.PriceScale+50000)

	var volume int64
	if err := pool.QueryRow(ctx,
		`SELECT (volume * 10000)::bigint FROM events WHERE id = $1`, eventID,
	).Scan(&volume); err != nil {
		t.Fatal(err)
	}
	if volume != 50000 {
		t.Errorf("event volume = %d, want 50000", volume)
	}

	var txCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&txCount); err != nil {
		t.Fatal(err)
	}
	if txCount != 2 {
		t.Errorf("transactions = %d, want 2 (BUY + SELL legs)", txCount)
	}

	// ---- Duplicate trade is idempotent (P3.1) ----
	if err := injectTrade(ctx, client, itestStream, trade, token); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trades`).Scan(&txCount); err != nil {
		t.Fatal(err)
	}
	if txCount != 1 {
		t.Errorf("duplicate trade inserted %d rows, want 1", txCount)
	}

	// ---- Rollback: a failing trade must not partially write (P3.1) ----
	trade.ID = uuid.NewString()
	trade.MatchOrderID = uuid.NewString() // no such order row → FK violation

	if err := injectTrade(ctx, client, itestStream, trade, token); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-workerDone:
		if err == nil {
			t.Error("expected worker to stop with the FK-violation trade")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop after failing trade")
	}

	var remainTrades, remainTx int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM trades`).Scan(&remainTrades)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&remainTx)

	if remainTrades != 1 {
		t.Errorf("rolled-back trade left trades = %d, want 1", remainTrades)
	}
	if remainTx != 2 {
		t.Errorf("rolled-back trade left transactions = %d, want 2", remainTx)
	}

	t.Logf("P3.2 integration OK: event=%s order=%s trade=%s", eventID, orderID, tradeID)
}

func signupAndSignin(t *testing.T, base, email string) struct {
	id    string
	token string
} {
	t.Helper()

	resp := postJSON(t, base+"/auth/signup", map[string]any{
		"email":    email,
		"password": "password123",
	}, "")

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("signup %s = %d: %s", email, resp.StatusCode, readBodyString(t, resp))
	}
	resp.Body.Close()

	resp = postJSON(t, base+"/auth/signin", map[string]any{
		"email":    email,
		"password": "password123",
	}, "")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signin %s = %d: %s", email, resp.StatusCode, readBodyString(t, resp))
	}

	defer resp.Body.Close()

	var out struct {
		Token string `json:"token"`
		User  struct {
			ID string `json:"id"`
		} `json:"user"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.Token == "" || out.User.ID == "" {
		t.Fatal("signin response missing token or user id")
	}

	return struct {
		id    string
		token string
	}{id: out.User.ID, token: out.Token}
}

func createEventViaAPI(t *testing.T, base, token string) string {
	t.Helper()

	resp := postJSON(t, base+"/event", map[string]any{
		"title":       "Will Zeta win?",
		"description": "ITest event",
		"question":    "Will Zeta win?",
		"imageurl":    "https://example.com/i.png",
		"expiresAt":   time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	}, token)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create event = %d: %s", resp.StatusCode, readBodyString(t, resp))
	}

	defer resp.Body.Close()

	var out struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.Data.ID == "" {
		t.Fatal("create event response missing id")
	}

	return out.Data.ID
}

func createOrderViaAPI(t *testing.T, base, token string, body map[string]any) string {
	t.Helper()

	resp := postJSON(t, base+"/order", body, token)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create order = %d: %s", resp.StatusCode, readBodyString(t, resp))
	}

	defer resp.Body.Close()

	var out struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out.Data.Status
}
