package dbworker

import (
	"encoding/json"
	"fmt"

	"predix/internal/engine"
	"predix/internal/events"

	"github.com/google/uuid"
)

// Validate permanent input errors before touching Redis fencing or Postgres.
func validateEnvelope(env events.EventEnvelope) error {
	if env.ID == "" || env.PartitionID < 0 || len(env.Data) == 0 || string(env.Data) == "null" {
		return fmt.Errorf("event requires id, nonnegative partitionId, and data")
	}
	var ids []string
	switch env.Type {
	case events.EventOrderCreated:
		var order engine.Order
		if err := json.Unmarshal(env.Data, &order); err != nil {
			return fmt.Errorf("decode order: %w", err)
		}
		ids = []string{order.ID, order.EventID, order.UserID}
		if order.Quantity <= 0 || order.Price < 0 || order.Price > 10000 || order.FilledQuantity < 0 || order.RemainingQuantity < 0 || order.FilledQuantity > order.Quantity || order.RemainingQuantity != order.Quantity-order.FilledQuantity {
			return fmt.Errorf("invalid order quantities or price")
		}
		if (order.Side != engine.SideBuy && order.Side != engine.SideSell) || (order.Outcome != engine.OutcomeYes && order.Outcome != engine.OutcomeNo) || (order.OrderType != engine.OrderTypeLimit && order.OrderType != engine.OrderTypeMarket) {
			return fmt.Errorf("invalid order side, outcome, or type")
		}
		switch order.Status {
		case engine.StatusPending, engine.StatusPartial, engine.StatusFilled, engine.StatusCanceled, engine.StatusRejected:
		default:
			return fmt.Errorf("invalid order status")
		}
	case events.EventTradeExecuted:
		var trade engine.Trade
		if err := json.Unmarshal(env.Data, &trade); err != nil {
			return fmt.Errorf("decode trade: %w", err)
		}
		if err := validateTrade(&trade); err != nil {
			return err
		}
		ids = []string{trade.ID, trade.EventID, trade.OrderID, trade.MatchOrderID, trade.BuyerID, trade.SellerID}
	case events.EventOrderCanceled:
		var payload struct {
			OrderID string `json:"orderId"`
		}
		if err := json.Unmarshal(env.Data, &payload); err != nil {
			return fmt.Errorf("decode cancellation: %w", err)
		}
		ids = []string{payload.OrderID}
	case events.EventOrderStatus, events.EventDepthUpdated:
		// These notifications do not change the DB projection.
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(env.Data, &payload); err != nil || payload == nil {
			return fmt.Errorf("invalid notification payload")
		}
	default:
		return fmt.Errorf("unknown event type %q", env.Type)
	}
	for _, id := range ids {
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("invalid payload UUID: %w", err)
		}
	}
	return nil
}
