package engine

import (
	"encoding/json"
	"testing"

	"predix/pkg/redis"
)

func createMarket(t *testing.T, e *Engine, eventID string) {
	t.Helper()

	payload, _ := json.Marshal(map[string]string{"eventId": eventID})
	if resp := e.handleCreateEvent(payload); !resp.Success {
		t.Fatalf("create event failed: %s", resp.Error)
	}
}

func fundUser(t *testing.T, e *Engine, userID string) {
	t.Helper()

	payload, _ := json.Marshal(map[string]string{"userId": userID})
	if resp := e.handleUserCreated(payload); !resp.Success {
		t.Fatalf("user created failed: %s", resp.Error)
	}
}

func placeOrder(
	t *testing.T,
	e *Engine,
	orderID string,
	eventID string,
	userID string,
	orderType string,
	outcome string,
	side string,
	quantity int64,
	price int64,
) *redis.EngineResponse {
	t.Helper()

	payload, _ := json.Marshal(map[string]any{
		"orderId":   orderID,
		"eventId":   eventID,
		"userId":    userID,
		"orderType": orderType,
		"outcome":   outcome,
		"side":      side,
		"quantity":  quantity,
		"price":     price,
	})

	return e.handleCreateOrder(payload)
}

func TestHandleUserCreatedIdempotent(t *testing.T) {
	e := testEngine(t)

	fundUser(t, e, "u1")
	fundUser(t, e, "u1")

	available, reserved, ok := e.GetBalances("u1")
	if !ok {
		t.Fatal("balance missing after USER_CREATED")
	}

	if available != DefaultStartingBalance || reserved != 0 {
		t.Errorf(
			"balance = (%d, %d), want (%d, 0)",
			available,
			reserved,
			DefaultStartingBalance,
		)
	}
}

func TestReserveRejectsInsufficientFunds(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")

	// 200 * 1.0000 = 200.00 > 100.00 starting balance.
	resp := placeOrder(
		t, e, "ord-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideBuy,
		200, 10000,
	)
	if resp.Success {
		t.Fatal("expected rejection for insufficient funds")
	}

	available, reserved, _ := e.GetBalances("u1")
	if available != DefaultStartingBalance || reserved != 0 {
		t.Errorf(
			"rejected order must not lock funds: got (%d, %d)",
			available,
			reserved,
		)
	}
}

func TestSettlementMovesFundsAndShares(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")
	fundUser(t, e, "u2")

	// Seed u1 with shares so it can sell.
	e.positions[positionKey("u1", "evt", OutcomeYes)] = &Position{Available: 10}

	sellResp := placeOrder(
		t, e, "sell-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideSell,
		10, 5000,
	)
	if !sellResp.Success {
		t.Fatalf("sell failed: %s", sellResp.Error)
	}

	// Sell reservation: 10 shares frozen.
	avail, res, _ := e.GetPosition("u1", "evt", OutcomeYes)
	if avail != 0 || res != 10 {
		t.Fatalf("seller position before match = (%d, %d), want (0, 10)", avail, res)
	}

	buyResp := placeOrder(
		t, e, "buy-1", "evt", "u2",
		OrderTypeLimit, OutcomeYes, SideBuy,
		10, 6000,
	)
	if !buyResp.Success {
		t.Fatalf("buy failed: %s", buyResp.Error)
	}

	// Seller: shares consumed, cash credited at the resting price (5000).
	avail, res, _ = e.GetPosition("u1", "evt", OutcomeYes)
	if avail != 0 || res != 0 {
		t.Errorf("seller position after match = (%d, %d), want (0, 0)", avail, res)
	}

	sellerBal, _, _ := e.GetBalances("u1")
	if want := DefaultStartingBalance + 10*5000; sellerBal != want {
		t.Errorf("seller balance = %d, want %d", sellerBal, want)
	}

	// Buyer: shares received, cash debited the executed cost.
	buyerPos, res, _ := e.GetPosition("u2", "evt", OutcomeYes)
	if buyerPos != 10 || res != 0 {
		t.Errorf("buyer position = (%d, %d), want (10, 0)", buyerPos, res)
	}

	buyerBal, buyerReserved, _ := e.GetBalances("u2")
	if want := DefaultStartingBalance - 10*5000; buyerBal != want {
		t.Errorf("buyer balance = %d, want %d", buyerBal, want)
	}

	// Over-reservation (6000 - 5000) * 10 must be released.
	if buyerReserved != 0 {
		t.Errorf("buyer reserved = %d, want 0", buyerReserved)
	}
}

func TestCancelReleasesReservation(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")

	e.positions[positionKey("u1", "evt", OutcomeYes)] = &Position{Available: 10}

	placeOrder(
		t, e, "sell-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideSell,
		4, 5000,
	)

	avail, res, _ := e.GetPosition("u1", "evt", OutcomeYes)
	if avail != 6 || res != 4 {
		t.Fatalf("position after sell = (%d, %d), want (6, 4)", avail, res)
	}

	payload, _ := json.Marshal(map[string]string{"orderId": "sell-1"})
	if resp := e.handleCancelOrder(payload); !resp.Success {
		t.Fatalf("cancel failed: %s", resp.Error)
	}

	avail, res, _ = e.GetPosition("u1", "evt", OutcomeYes)
	if avail != 10 || res != 0 {
		t.Errorf("position after cancel = (%d, %d), want (10, 0)", avail, res)
	}
}

func TestSelfTradePrevention(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")

	e.positions[positionKey("u1", "evt", OutcomeYes)] = &Position{Available: 10}

	// u1 rests a sell.
	placeOrder(
		t, e, "sell-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideSell,
		5, 5000,
	)

	// Same user buys at a crossing price: must not self-match.
	buyResp := placeOrder(
		t, e, "buy-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideBuy,
		5, 6000,
	)
	if !buyResp.Success {
		t.Fatalf("buy failed: %s", buyResp.Error)
	}

	var result struct {
		Status string `json:"status"`
	}
	json.Unmarshal(buyResp.Data, &result)

	if result.Status != StatusPending {
		t.Errorf("self-crossing buy status = %s, want PENDING (no self match)", result.Status)
	}

	sell := e.orders["sell-1"]
	if sell.FilledQuantity != 0 {
		t.Errorf("resting self order filled = %d, want 0", sell.FilledQuantity)
	}

	book := e.markets["evt"].Book(OutcomeYes)
	if len(book.Asks) != 1 || len(book.Bids) != 1 {
		t.Errorf("book = %d asks / %d bids, want 1 / 1", len(book.Asks), len(book.Bids))
	}
}

func TestMarketOrderPartialIsCanceledAndReleases(t *testing.T) {
	e := testEngine(t)
	createMarket(t, e, "evt")
	fundUser(t, e, "u1")
	fundUser(t, e, "u2")

	e.positions[positionKey("u1", "evt", OutcomeYes)] = &Position{Available: 3}

	placeOrder(
		t, e, "sell-1", "evt", "u1",
		OrderTypeLimit, OutcomeYes, SideSell,
		3, 5000,
	)

	// Market buy for 10 with only 3 available: fills 3, cancels the rest.
	resp := placeOrder(
		t, e, "buy-1", "evt", "u2",
		OrderTypeMarket, OutcomeYes, SideBuy,
		10, 0,
	)
	if !resp.Success {
		t.Fatalf("market buy failed: %s", resp.Error)
	}

	order := e.orders["buy-1"]
	if order.Status != StatusCanceled {
		t.Errorf("market partial status = %s, want CANCELED", order.Status)
	}

	buyerBal, buyerReserved, _ := e.GetBalances("u2")
	if want := DefaultStartingBalance - 3*5000; buyerBal != want {
		t.Errorf("buyer balance = %d, want %d", buyerBal, want)
	}

	if buyerReserved != 0 {
		t.Errorf("buyer reserved = %d, want 0 (unused lock released)", buyerReserved)
	}
}
