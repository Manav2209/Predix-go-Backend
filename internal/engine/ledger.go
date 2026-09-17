package engine

import (
	"encoding/json"

	"predix/pkg/redis"
)

// handleUserCreated opens a user's ledger entry (or a no-op if it already
// exists). The API emits this command after a successful signup so the engine
// grants the starting balance.
//
// It is idempotent, which makes it safe to re-execute during replay.
func (e *Engine) handleUserCreated(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		UserID string `json:"userId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	if req.UserID == "" {
		return failure("userId is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Grant the starting balance only on first creation so replaying the
	// command cannot top up an existing account.
	if _, exists := e.balances[req.UserID]; !exists {
		e.balances[req.UserID] = &Balance{
			Available: DefaultStartingBalance,
		}
	}

	return successJSON(map[string]any{
		"status": "ok",
	})
}

// getOrCreateBalance returns a user's cash balance, creating a zeroed entry
// when absent. Callers must hold e.mu.
//
// A zeroed entry is deliberate: new users are only funded by the
// USER_CREATED command, not by placing an order.
func (e *Engine) getOrCreateBalance(userID string) *Balance {
	bal, ok := e.balances[userID]
	if !ok {
		bal = &Balance{}
		e.balances[userID] = bal
	}
	return bal
}

// getOrCreatePosition returns a user's position for one (event, outcome).
// Callers must hold e.mu.
func (e *Engine) getOrCreatePosition(
	userID string,
	eventID string,
	outcome string,
) *Position {

	key := positionKey(userID, eventID, outcome)

	pos, ok := e.positions[key]
	if !ok {
		pos = &Position{}
		e.positions[key] = pos
	}
	return pos
}

func positionKey(
	userID string,
	eventID string,
	outcome string,
) string {
	return userID + ":" + eventID + ":" + outcome
}

// reserveOrderLocked freezes the liquidity an order needs before it enters
// the book: cost (qty * limit) for a BUY, or shares (qty) for a SELL. The
// frozen amount is tracked on order.LockedFunds and released as fills occur
// (settleTradeLocked) and on cancel/complete (releaseOrderLocked).
//
// A market BUY has no price, so it temporarily freezes the user's entire
// available balance; the unused portion is released after matching.
func (e *Engine) reserveOrderLocked(order *Order) error {

	if order.Side == SideBuy {

		bal := e.getOrCreateBalance(order.UserID)

		var cost int64
		if order.OrderType == OrderTypeMarket {
			cost = bal.Available
		} else {
			cost = order.RemainingQuantity * order.Price
		}

		if cost > bal.Available {
			return ErrInsufficientFunds
		}

		bal.Available -= cost
		bal.Reserved += cost
		order.LockedFunds = cost

		return nil
	}

	// SELL
	pos := e.getOrCreatePosition(
		order.UserID,
		order.EventID,
		order.Outcome,
	)

	if pos.Available < order.RemainingQuantity {
		return ErrInsufficientShares
	}

	pos.Available -= order.RemainingQuantity
	pos.Reserved += order.RemainingQuantity
	order.LockedFunds = order.RemainingQuantity

	return nil
}

// settleTradeLocked applies one executed trade to the ledger:
//   - buyer: reserved cash decreases by the executed cost; shares increase.
//   - seller: reserved shares decrease by the quantity; cash increases.
//
// The corresponding order.LockedFunds are reduced so the eventual release is
// exact. Callers must hold e.mu.
func (e *Engine) settleTradeLocked(trade *Trade) {

	cost := trade.Quantity * trade.Price

	buyerBal := e.getOrCreateBalance(trade.BuyerID)
	buyerBal.Reserved -= cost
	if buyerBal.Reserved < 0 {
		buyerBal.Reserved = 0
	}

	buyerPos := e.getOrCreatePosition(
		trade.BuyerID,
		trade.EventID,
		trade.Outcome,
	)
	buyerPos.Available += trade.Quantity

	sellerBal := e.getOrCreateBalance(trade.SellerID)
	sellerBal.Available += cost

	sellerPos := e.getOrCreatePosition(
		trade.SellerID,
		trade.EventID,
		trade.Outcome,
	)
	sellerPos.Reserved -= trade.Quantity
	if sellerPos.Reserved < 0 {
		sellerPos.Reserved = 0
	}

	for _, orderID := range []string{trade.OrderID, trade.MatchOrderID} {

		order, ok := e.orders[orderID]
		if !ok {
			continue
		}

		if order.Side == SideBuy {
			order.LockedFunds -= cost
		} else {
			order.LockedFunds -= trade.Quantity
		}

		if order.LockedFunds < 0 {
			order.LockedFunds = 0
		}
	}
}

// releaseOrderLocked returns any liquidity that is no longer needed because
// an order was canceled or fully consumed. Callers must hold e.mu.
func (e *Engine) releaseOrderLocked(order *Order) {

	if order.LockedFunds <= 0 {
		return
	}

	if order.Side == SideBuy {

		bal := e.getOrCreateBalance(order.UserID)
		bal.Reserved -= order.LockedFunds
		if bal.Reserved < 0 {
			bal.Reserved = 0
		}
		bal.Available += order.LockedFunds

	} else {

		pos := e.getOrCreatePosition(
			order.UserID,
			order.EventID,
			order.Outcome,
		)
		pos.Reserved -= order.LockedFunds
		if pos.Reserved < 0 {
			pos.Reserved = 0
		}
		pos.Available += order.LockedFunds
	}

	order.LockedFunds = 0
}

// GetBalances returns a snapshot of one user's cash holdings.
func (e *Engine) GetBalances(userID string) (
	available int64,
	reserved int64,
	ok bool,
) {

	e.mu.RLock()
	defer e.mu.RUnlock()

	bal, exists := e.balances[userID]
	if !exists {
		return 0, 0, false
	}

	return bal.Available, bal.Reserved, true
}

// GetPosition returns a snapshot of one user's shares for an (event, outcome).
func (e *Engine) GetPosition(
	userID string,
	eventID string,
	outcome string,
) (available int64, reserved int64, ok bool) {

	e.mu.RLock()
	defer e.mu.RUnlock()

	pos, exists := e.positions[positionKey(userID, eventID, outcome)]
	if !exists {
		return 0, 0, false
	}

	return pos.Available, pos.Reserved, true
}