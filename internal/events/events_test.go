package events

import (
	"encoding/json"
	"testing"
)

func TestNewEnvelopeRoundTrip(t *testing.T) {
	env := NewEnvelope(NewEnvelopeParams{
		Type:        EventTradeExecuted,
		Data:        []byte(`{"quantity":10,"price":5000}`),
		PartitionID: 0,
		Sequence:    7,
	})

	if env.ID == "" {
		t.Error("envelope id must be populated")
	}

	if env.Type != EventTradeExecuted {
		t.Errorf("type = %s, want trade_executed", env.Type)
	}

	if env.PartitionID != 0 {
		t.Errorf("partition = %d, want 0", env.PartitionID)
	}

	if env.Sequence != 7 {
		t.Errorf("sequence = %d, want 7", env.Sequence)
	}

	if env.CreatedAt.IsZero() {
		t.Error("createdAt must be populated")
	}

	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var back EventEnvelope
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if back.ID != env.ID {
		t.Errorf("id round trip = %q, want %q", back.ID, env.ID)
	}

	if back.Type != env.Type {
		t.Errorf("type round trip = %q, want %q", back.Type, env.Type)
	}

	if back.Sequence != env.Sequence {
		t.Errorf("sequence round trip = %d, want %d", back.Sequence, env.Sequence)
	}

	if string(back.Data) != string(env.Data) {
		t.Errorf("data round trip = %s, want %s", back.Data, env.Data)
	}
}

func TestEventTypeConstants(t *testing.T) {
	cases := map[EventType]string{
		EventTradeExecuted: "trade_executed",
		EventOrderCreated:  "order_created",
		EventOrderStatus:   "order_status",
		EventOrderCanceled: "order_canceled",
		EventDepthUpdated:  "depth_updated",
	}

	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("constant = %q, want %q", typ, want)
		}
	}
}
