package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestAwaitResponseCorrelatesByRequestID(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	rm := NewRedisManager(srv.Addr(), "")
	defer rm.Close()

	client := rm.GetClient()

	clientID := "rpc-" + generateClientID()

	sub := client.Subscribe(context.Background(), clientID)
	defer sub.Close()

	if _, err := sub.Receive(context.Background()); err != nil {
		t.Fatalf("subscribe confirm: %v", err)
	}

	// A stray response with the wrong requestId must be ignored.
	client.Publish(context.Background(), clientID, mustString(t, EngineResponse{
		Success:   true,
		RequestID: "stray-id",
		Data:      json.RawMessage(`{"x":1}`),
	}))

	// The correlated reply then arrives.
	client.Publish(context.Background(), clientID, mustString(t, EngineResponse{
		Success:   true,
		RequestID: "want-1",
		Data:      json.RawMessage(`{"y":2}`),
	}))

	resp, err := rm.awaitResponse(context.Background(), sub, "want-1", time.Second)
	if err != nil {
		t.Fatalf("awaitResponse: %v", err)
	}

	if !resp.Success {
		t.Error("expected success response")
	}

	if resp.RequestID != "want-1" {
		t.Errorf("correlated requestId = %s, want want-1", resp.RequestID)
	}

	if string(resp.Data) != `{"y":2}` {
		t.Errorf("data = %s, want {\"y\":2}", resp.Data)
	}
}

func TestSendAndAwaitTimesOutGracefully(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	rm := NewRedisManager(srv.Addr(), "")
	defer rm.Close()

	start := time.Now()

	// No engine answers; the bounded deadline must return instead of leaking.
	_, err = rm.SendAndAwaitWithTimeout(
		context.Background(),
		"evt-1",
		MessageToEngine{
			Type:    string(CreateOrderCommand),
			Payload: json.RawMessage(`{}`),
		},
		150*time.Millisecond,
	)

	if err == nil {
		t.Fatal("expected a timeout error")
	}

	if time.Since(start) > 2*time.Second {
		t.Errorf("SendAndAwait took too long to time out: %v", time.Since(start))
	}
}

func mustString(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
