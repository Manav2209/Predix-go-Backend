package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewJSONWithInstanceID(t *testing.T) {
	var buf bytes.Buffer

	l := New("engine-7", slog.LevelInfo, &buf)

	l.Info("command_handled",
		"type", "CREATE_ORDER",
		"requestId", "req-1",
		"commandId", "cmd-1",
		"eventId", "evt-1",
		"orderId", "ord-1",
		"partitionId", 2,
		"sequence", 42,
	)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, buf.String())
	}

	if rec["instanceId"] != "engine-7" {
		t.Errorf("instanceId = %v, want engine-7", rec["instanceId"])
	}

	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}

	if rec["msg"] != "command_handled" {
		t.Errorf("msg = %v, want command_handled", rec["msg"])
	}

	for _, k := range []string{
		"type", "requestId", "commandId",
		"eventId", "orderId", "partitionId", "sequence",
	} {
		if _, ok := rec[k]; !ok {
			t.Errorf("record missing field %q: %v", k, rec)
		}
	}
}

func TestInstanceIDDefault(t *testing.T) {
	var buf bytes.Buffer

	l := New("", slog.LevelInfo, &buf)

	l.Info("ping")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}

	if rec["instanceId"] == "" || rec["instanceId"] == nil {
		t.Error("expected non-empty default instanceId")
	}
}
