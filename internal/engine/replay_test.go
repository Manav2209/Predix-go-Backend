package engine

import (
	"encoding/json"
	"testing"

	"predix/pkg/redis"
)

func envelope(t *testing.T, typ redis.CommandType, payload any) redis.CommandEnvelope {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	return redis.CommandEnvelope{
		CommandID: "cmd-" + string(typ),
		Type:      string(typ),
		Payload:   raw,
	}
}

func applyAll(t *testing.T, e *Engine, commands []redis.CommandEnvelope) {
	t.Helper()

	e.mu.Lock()
	e.replaying = true
	e.mu.Unlock()

	for _, env := range commands {
		e.applyCommand(env)
	}

	e.mu.Lock()
	e.replaying = false
	e.mu.Unlock()
}

func TestReplayReconstructsStateDeterministically(t *testing.T) {
	commands := []redis.CommandEnvelope{
		envelope(t, redis.UserCreatedCommand, map[string]string{"userId": "u1"}),
		envelope(t, redis.CreateEventCommand, map[string]string{"eventId": "evt"}),
		envelope(t, redis.CreateOrderCommand, map[string]any{
			"orderId":   "buy-1",
			"eventId":   "evt",
			"userId":    "u1",
			"orderType": OrderTypeLimit,
			"outcome":   OutcomeYes,
			"side":      SideBuy,
			"quantity":  int64(5),
			"price":     int64(6000),
		}),
	}

	build := func() *Engine {
		e := testEngine(t)
		applyAll(t, e, commands)
		return e
	}

	first := build()
	second := build()

	// Balance reconstruction: 100.00 starting balance minus 5 * 0.6000.
	avail, reserved, ok := first.GetBalances("u1")
	if !ok {
		t.Fatal("balance not reconstructed")
	}

	if avail != DefaultStartingBalance-5*6000 || reserved != 5*6000 {
		t.Errorf("balance = (%d, %d), want (%d, %d)",
			avail, reserved,
			DefaultStartingBalance-5*6000, 5*6000,
		)
	}

	// Order/book reconstruction.
	order, ok := first.orders["buy-1"]
	if !ok {
		t.Fatal("order not reconstructed")
	}

	if order.Status != StatusPending {
		t.Errorf("order status = %s, want PENDING", order.Status)
	}

	if book := first.markets["evt"].Book(OutcomeYes); len(book.Bids) != 1 {
		t.Errorf("bids reconstructed = %d, want 1", len(book.Bids))
	}

	// Determinism: replaying the same sequence yields identical state.
	secondAvail, secondReserved, _ := second.GetBalances("u1")
	if secondAvail != avail || secondReserved != reserved {
		t.Errorf("replay not deterministic: (%d, %d) vs (%d, %d)",
			secondAvail, secondReserved, avail, reserved,
		)
	}
}

func TestReplayDuplicateCommandIsIdempotent(t *testing.T) {
	commands := []redis.CommandEnvelope{
		envelope(t, redis.UserCreatedCommand, map[string]string{"userId": "u1"}),
		envelope(t, redis.CreateEventCommand, map[string]string{"eventId": "evt"}),
		envelope(t, redis.CreateOrderCommand, map[string]any{
			"orderId":   "buy-1",
			"eventId":   "evt",
			"userId":    "u1",
			"orderType": OrderTypeLimit,
			"outcome":   OutcomeYes,
			"side":      SideBuy,
			"quantity":  int64(5),
			"price":     int64(6000),
		}),
		// Duplicate delivery of the same command must not double-reserve.
		envelope(t, redis.CreateOrderCommand, map[string]any{
			"orderId":   "buy-1",
			"eventId":   "evt",
			"userId":    "u1",
			"orderType": OrderTypeLimit,
			"outcome":   OutcomeYes,
			"side":      SideBuy,
			"quantity":  int64(5),
			"price":     int64(6000),
		}),
	}

	e := testEngine(t)
	applyAll(t, e, commands)

	avail, reserved, _ := e.GetBalances("u1")
	if avail != DefaultStartingBalance-5*6000 || reserved != 5*6000 {
		t.Errorf("duplicate command double-applied: (%d, %d)", avail, reserved)
	}

	if book := e.markets["evt"].Book(OutcomeYes); len(book.Bids) != 1 {
		t.Errorf("bids = %d, want 1", len(book.Bids))
	}
}
