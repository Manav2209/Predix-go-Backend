package engine

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"predix/internal/events"
	rpkg "predix/pkg/redis"
)

func TestPriceValidationProtectsCashReservations(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")
	for _, tc := range []struct{ quantity, price int64 }{{1, 10001}, {1, -1}, {math.MaxInt64, 10000}} {
		if response := placeOrder(t, e, "bad", "evt", "u1", "LIMIT", "YES", "BUY", tc.quantity, tc.price); response.Success {
			t.Fatal("invalid order accepted")
		}
	}
	available, reserved, _ := e.GetBalances("u1")
	if available != DefaultStartingBalance || reserved != 0 {
		t.Fatal("invalid order changed cash")
	}
}

func TestExpiredMarketRejectsLiveOrdersButReplaysHistory(t *testing.T) {
	e := testEngine(t)
	fundUser(t, e, "u1")
	payload, _ := json.Marshal(map[string]any{"eventId": "evt", "expiresAt": time.Now().Add(-time.Hour)})
	if response := e.handleCreateEvent(payload); !response.Success {
		t.Fatal(response.Error)
	}
	if response := placeOrder(t, e, "live", "evt", "u1", "LIMIT", "YES", "BUY", 1, 5000); response.Success {
		t.Fatal("expired market accepted live order")
	}
	e.replaying[0] = true
	if response := placeOrder(t, e, "historical", "evt", "u1", "LIMIT", "YES", "BUY", 1, 5000); !response.Success {
		t.Fatalf("historical order rejected during replay: %s", response.Error)
	}
}

func TestMarketCancellationAndDepthAreEmittedInOrder(t *testing.T) {
	_, rm := startTestRedisForEngine(t)
	e := newTestEngine(t, rm)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")
	response := placeOrder(t, e, "market", "evt", "u1", "MARKET", "YES", "BUY", 1, 0)
	if !response.Success {
		t.Fatal(response.Error)
	}
	entries, err := rm.GetClient().XRange(context.Background(), events.EventStream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	want := []events.EventType{events.EventOrderCreated, events.EventOrderCanceled, events.EventDepthUpdated}
	if len(entries) != len(want) {
		t.Fatalf("events=%d want=%d", len(entries), len(want))
	}
	for i, entry := range entries {
		var env events.EventEnvelope
		if err := json.Unmarshal([]byte(entry.Values["event"].(string)), &env); err != nil {
			t.Fatal(err)
		}
		if env.Type != want[i] {
			t.Fatalf("event %d=%s want=%s", i, env.Type, want[i])
		}
	}
	available, reserved, _ := e.GetBalances("u1")
	if available != DefaultStartingBalance || reserved != 0 {
		t.Fatal("unfilled market buy retained cash reservation")
	}
	// Every emitted event is suppressed during command-log recovery.
	e.replaying[0] = true
	e.applyCommand(0, envelope(t, rpkg.CreateOrderCommand, map[string]any{"orderId": "replay-market", "eventId": "evt", "userId": "u1", "orderType": "MARKET", "outcome": "YES", "side": "BUY", "quantity": 1}))
	if rm.GetClient().XLen(context.Background(), events.EventStream).Val() != int64(len(want)) {
		t.Fatal("replay emitted lifecycle effects")
	}
}
