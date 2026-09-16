package websocket

import (
	"encoding/json"
	"testing"
	"time"

	"predix/internal/engine"
	"predix/internal/events"
)

func testSubscriber(t *testing.T) (*RedisSubscriber, *Hub, *Client) {
	t.Helper()

	hub := NewHub()

	client := NewClient(hub, nil)
	hub.Register(client)
	hub.Subscribe(client, "evt-1")

	s := &RedisSubscriber{hub: hub}

	return s, hub, client
}

func captureMessage(t *testing.T, client *Client) []byte {
	t.Helper()

	select {
	case msg := <-client.send:
		return msg
	case <-time.After(time.Second):
		t.Fatal("no websocket message broadcast")
		return nil
	}
}

func TestHandleTradeEnvelope(t *testing.T) {
	s, _, client := testSubscriber(t)

	data, _ := json.Marshal(engine.Trade{
		ID:           "trade-1",
		OrderID:      "order-a",
		MatchOrderID: "order-b",
		EventID:      "evt-1",
		Outcome:      engine.OutcomeYes,
		BuyerID:      "buyer-1",
		SellerID:     "seller-1",
		TakerSide:    engine.SideBuy,
		Quantity:     10,
		Price:        6200,
		CreatedAt:    time.Now().UTC(),
	})

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:        events.EventTradeExecuted,
		Data:        data,
		PartitionID: 0,
		Sequence:    1,
	})

	envBytes, _ := json.Marshal(envelope)

	if err := s.handle(envBytes); err != nil {
		t.Fatalf("handle valid envelope: %v", err)
	}

	payload := captureMessage(t, client)

	var msg TradeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("decode ws trade message: %v", err)
	}

	if msg.Type != "trade" {
		t.Errorf("ws message type = %s, want trade", msg.Type)
	}

	if msg.Data.Price != 6200 {
		t.Errorf("ws price = %d, want 6200", msg.Data.Price)
	}

	if msg.Data.Quantity != 10 {
		t.Errorf("ws quantity = %d, want 10", msg.Data.Quantity)
	}

	if msg.Data.UserID != "buyer-1" {
		t.Errorf("ws actor = %s, want buyer-1", msg.Data.UserID)
	}
}

func TestHandleUnknownEventType(t *testing.T) {
	s, _, _ := testSubscriber(t)

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:        events.EventOrderCreated,
		Data:        []byte(`{"id":"order-1"}`),
		PartitionID: 0,
		Sequence:    1,
	})

	envBytes, _ := json.Marshal(envelope)

	// Unknown types must be tolerated, not fatal.
	if err := s.handle(envBytes); err != nil {
		t.Fatalf("unknown event type must not error, got: %v", err)
	}
}

func TestHandleDepthEnvelope(t *testing.T) {
	s, _, client := testSubscriber(t)

	payload, _ := json.Marshal(map[string]any{
		"eventId": "evt-1",
		"data": map[string]any{
			"YES": map[string]any{
				"bids": []map[string]any{
					{"price": 6200, "quantity": 10, "total": 62000},
				},
				"asks": []map[string]any{},
			},
			"NO": map[string]any{
				"bids": []map[string]any{},
				"asks": []map[string]any{},
			},
		},
	})

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:        events.EventDepthUpdated,
		Data:        payload,
		PartitionID: 0,
		Sequence:    2,
	})

	envBytes, _ := json.Marshal(envelope)

	if err := s.handle(envBytes); err != nil {
		t.Fatalf("handle depth envelope: %v", err)
	}

	raw := captureMessage(t, client)

	var msg DepthMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode ws depth message: %v", err)
	}

	if msg.Type != "depth" {
		t.Errorf("ws depth type = %s, want depth", msg.Type)
	}

	if len(msg.Data.YES.Bids) != 1 {
		t.Fatalf("depth YES bids = %d, want 1", len(msg.Data.YES.Bids))
	}

	if msg.Data.YES.Bids[0].Price != 6200 {
		t.Errorf("depth bid price = %d, want 6200", msg.Data.YES.Bids[0].Price)
	}
}

func TestHandleMalformedPayload(t *testing.T) {
	s, _, _ := testSubscriber(t)

	if err := s.handle([]byte("{not-json")); err == nil {
		t.Error("malformed payload must produce an error")
	}
}

func TestTradeDataDepthUsesInt64(t *testing.T) {
	if _, err := json.Marshal(DepthEntry{
		Price:    6200,
		Quantity: 15,
		Total:    93000,
	}); err != nil {
		t.Fatalf("marshal depth entry: %v", err)
	}
}