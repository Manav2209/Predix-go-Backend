package engine

import (
	"math"
	"testing"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()

	// redisManager is left nil: emitEvent no-ops, so matching exercises
	// the book without a live Redis.
	return &Engine{
		markets:      make(map[string]*Market),
		orders:       make(map[string]*Order),
		pendingQueue: make(chan *Order, 1),
	}
}

func TestScalePriceUnscalePrice(t *testing.T) {
	if PriceScale != 10000 {
		t.Fatalf("PriceScale = %d, want 10000", PriceScale)
	}

	cases := []struct {
		in   float64
		want int64
	}{
		{0, 0},
		{0.437, 4370},
		{1, 10000},
		{0.5, 5000},
	}

	for _, c := range cases {
		if got := ScalePrice(c.in); got != c.want {
			t.Errorf("ScalePrice(%v) = %d, want %d", c.in, got, c.want)
		}
	}

	for _, c := range cases {
		back := UnscalePrice(ScalePrice(c.in))
		if math.Abs(back-c.in) > 0.0001 {
			t.Errorf("round trip %v = %v", c.in, back)
		}
	}
}

func TestCanMatch(t *testing.T) {
	limitBuy := func(price int64) *Order {
		return &Order{
			OrderType: OrderTypeLimit,
			Side:      SideBuy,
			Outcome:   OutcomeYes,
			Price:     price,
		}
	}

	limitSell := func(price int64) *Order {
		return &Order{
			OrderType: OrderTypeLimit,
			Side:      SideSell,
			Outcome:   OutcomeYes,
			Price:     price,
		}
	}

	if !canMatch(limitBuy(6000), limitSell(5000)) {
		t.Error("equal- or lower-priced ask should match a buy")
	}

	if !canMatch(limitBuy(5000), limitSell(5000)) {
		t.Error("equal-price should match")
	}

	if canMatch(limitBuy(4000), limitSell(5000)) {
		t.Error("higher-priced ask should not match a buy")
	}

	if !canMatch(limitSell(5000), limitBuy(6000)) {
		t.Error("equal- or higher-priced bid should match a sell")
	}

	if canMatch(limitSell(7000), limitBuy(6000)) {
		t.Error("lower-priced bid should not match a sell")
	}

	if canMatch(
		&Order{
			OrderType: OrderTypeLimit,
			Side:      SideBuy,
			Outcome:   OutcomeNo,
			Price:     6000,
		},
		limitSell(5000),
	) {
		t.Error("outcome mismatch must not match")
	}
}

func TestMatchOrderFillsAgainstRestingAsk(t *testing.T) {
	e := testEngine(t)

	e.markets["evt"] = NewMarket()
	book := e.markets["evt"].Book(OutcomeYes)

	resting := &Order{
		ID: "maker-1", EventID: "evt", UserID: "u1",
		OrderType: OrderTypeLimit, Outcome: OutcomeYes, Side: SideSell,
		Quantity: 10, RemainingQuantity: 10,
		Price:  5000,
		Status: StatusPending,
	}

	book.Asks = append(book.Asks, resting)

	incoming := &Order{
		ID: "taker-1", EventID: "evt", UserID: "u2",
		OrderType: OrderTypeLimit, Outcome: OutcomeYes, Side: SideBuy,
		Quantity: 10, RemainingQuantity: 10,
		Price:  6000,
		Status: StatusPending,
	}

	e.matchOrder(incoming)

	if incoming.Status != StatusFilled {
		t.Errorf("taker status = %s, want FILLED", incoming.Status)
	}

	if incoming.RemainingQuantity != 0 {
		t.Errorf("taker remaining = %d, want 0", incoming.RemainingQuantity)
	}

	if resting.Status != StatusFilled {
		t.Errorf("maker status = %s, want FILLED", resting.Status)
	}

	if resting.RemainingQuantity != 0 {
		t.Errorf("maker remaining = %d, want 0", resting.RemainingQuantity)
	}

	if len(book.Asks) != 0 {
		t.Errorf("asks left on book = %d, want 0", len(book.Asks))
	}
}

func TestMatchOrderPartialLeavesMakerOnBook(t *testing.T) {
	e := testEngine(t)

	e.markets["evt"] = NewMarket()
	book := e.markets["evt"].Book(OutcomeYes)

	resting := &Order{
		ID: "maker-1", EventID: "evt", UserID: "u1",
		OrderType: OrderTypeLimit, Outcome: OutcomeYes, Side: SideSell,
		Quantity: 10, RemainingQuantity: 10,
		Price:  5000,
		Status: StatusPending,
	}

	book.Asks = append(book.Asks, resting)

	incoming := &Order{
		ID: "taker-1", EventID: "evt", UserID: "u2",
		OrderType: OrderTypeLimit, Outcome: OutcomeYes, Side: SideBuy,
		Quantity: 4, RemainingQuantity: 4,
		Price:  6000,
		Status: StatusPending,
	}

	e.matchOrder(incoming)

	if incoming.Status != StatusFilled {
		t.Errorf("taker status = %s, want FILLED", incoming.Status)
	}

	if resting.Status != StatusPartial {
		t.Errorf("maker status = %s, want PARTIAL", resting.Status)
	}

	if resting.RemainingQuantity != 6 {
		t.Errorf("maker remaining = %d, want 6", resting.RemainingQuantity)
	}

	if len(book.Asks) != 1 {
		t.Errorf("asks on book = %d, want 1 (remaining limit rests)", len(book.Asks))
	}
}

func TestAggregateFixedPointDepth(t *testing.T) {
	e := testEngine(t)

	e.markets["evt"] = NewMarket()
	book := e.markets["evt"].Book(OutcomeYes)

	resting := []*Order{
		{
			ID: "ask-1", EventID: "evt", OrderType: OrderTypeLimit,
			Outcome: OutcomeYes, Side: SideSell,
			Quantity: 10, RemainingQuantity: 10, Price: 5000, Status: StatusPending,
		},
		{
			ID: "ask-2", EventID: "evt", OrderType: OrderTypeLimit,
			Outcome: OutcomeYes, Side: SideSell,
			Quantity: 5, RemainingQuantity: 5, Price: 5000, Status: StatusPending,
		},
		{
			ID: "ask-3", EventID: "evt", OrderType: OrderTypeLimit,
			Outcome: OutcomeYes, Side: SideSell,
			Quantity: 7, RemainingQuantity: 7, Price: 6000, Status: StatusPending,
		},
	}

	book.Asks = append(book.Asks, resting...)

	levels := aggregateAsks(book.Asks)

	if len(levels) != 2 {
		t.Fatalf("depth levels = %d, want 2", len(levels))
	}

	// Lowest ask first.
	if levels[0].Price != 5000 {
		t.Errorf("first level price = %d, want 5000", levels[0].Price)
	}

	if levels[0].Quantity != 15 {
		t.Errorf("first level quantity = %d, want 15", levels[0].Quantity)
	}

	if levels[0].Total != 5000*15 {
		t.Errorf("first level total = %d, want %d", levels[0].Total, 5000*15)
	}

	if levels[0].OrderCount != 2 {
		t.Errorf("first level orderCount = %d, want 2", levels[0].OrderCount)
	}

	if levels[1].Price != 6000 {
		t.Errorf("second level price = %d, want 6000", levels[1].Price)
	}

	if levels[1].OrderCount != 1 {
		t.Errorf("second level orderCount = %d, want 1", levels[1].OrderCount)
	}
}