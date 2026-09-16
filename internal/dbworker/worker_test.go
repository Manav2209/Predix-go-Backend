package dbworker

import (
	"errors"
	"math/big"
	"testing"

	"predix/internal/engine"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestIsUniqueViolation(t *testing.T) {
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Error("23505 should be detected as a unique violation")
	}

	if isUniqueViolation(&pgconn.PgError{Code: "23000"}) {
		t.Error("other codes must not be detected")
	}

	if isUniqueViolation(errors.New("boom")) {
		t.Error("non-pg errors must not be detected")
	}

	if isUniqueViolation(nil) {
		t.Error("nil must not be detected")
	}
}

func TestNumericHelpers(t *testing.T) {
	n := numericFromInt(5)
	if !n.Valid || n.Int.Int64() != 5 || n.Exp != 0 {
		t.Errorf("numericFromInt(5) = valid=%v int=%v exp=%d",
			n.Valid, n.Int.Int64(), n.Exp)
	}

	n = numericFromScaled(4370)
	if !n.Valid || n.Int.Int64() != 4370 || n.Exp != -4 {
		t.Errorf("numericFromScaled(4370) = valid=%v int=%v exp=%d",
			n.Valid, n.Int.Int64(), n.Exp)
	}

	n = numericFromScaled(10000)
	if !n.Valid || n.Int.Int64() != 10000 || n.Exp != -4 {
		t.Errorf("numericFromScaled(10000) = valid=%v int=%v exp=%d",
			n.Valid, n.Int.Int64(), n.Exp)
	}

	f, err := numericToInt64(pgtype.Numeric{
		Int:   big.NewInt(7),
		Exp:   0,
		Valid: true,
	})
	if err != nil || f != 7 {
		t.Errorf("numericToInt64(7) = %d, %v; want 7, nil", f, err)
	}

	f, err = numericToInt64(pgtype.Numeric{Valid: false})
	if err != nil || f != 0 {
		t.Errorf("numericToInt64(invalid) = %d, %v; want 0, nil", f, err)
	}
}

func TestValidateTrade(t *testing.T) {
	ok := &engine.Trade{
		ID:           "trade-1",
		EventID:      "evt-1",
		OrderID:      "taker-1",
		MatchOrderID: "maker-1",
		BuyerID:      "buyer-1",
		SellerID:     "seller-1",
		Outcome:      engine.OutcomeYes,
		TakerSide:    engine.SideBuy,
		Quantity:     10,
		Price:        5000,
	}

	if err := validateTrade(ok); err != nil {
		t.Errorf("valid trade rejected: %v", err)
	}

	bad := []struct {
		name  string
		mut   func(t *engine.Trade)
		check string
	}{
		{"missing id", func(t *engine.Trade) { t.ID = "" }, "trade id"},
		{"zero quantity", func(t *engine.Trade) { t.Quantity = 0 }, "quantity"},
		{"negative quantity", func(t *engine.Trade) { t.Quantity = -1 }, "quantity"},
		{"negative price", func(t *engine.Trade) { t.Price = -1 }, "price"},
		{"bad outcome", func(t *engine.Trade) { t.Outcome = "MAYBE" }, "outcome"},
	}

	for _, c := range bad {
		trade := *ok
		c.mut(&trade)

		if err := validateTrade(&trade); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
}