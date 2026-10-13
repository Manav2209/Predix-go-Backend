package dbworker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"predix/internal/events"
	"predix/internal/partition"
)

func recoveryWorker(t *testing.T) (*Worker, *redis.Client) {
	t.Helper()
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	w := New(client, nil, nil)
	w.claimIdle = 0
	w.claimInterval = 10 * time.Millisecond
	w.readBlock = 10 * time.Millisecond
	if err := w.ensureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	return w, client
}

func notification(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(events.NewEnvelope(events.NewEnvelopeParams{Type: events.EventOrderStatus, Data: []byte(`{}`)}))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func abandon(t *testing.T, w *Worker, client *redis.Client, count int, raw string) {
	t.Helper()
	ctx := context.Background()
	// Append and assign pending ownership atomically so the running worker
	// cannot win the race to read these entries as new messages.
	if err := client.Eval(ctx, `
for i = 1, tonumber(ARGV[1]) do redis.call('XADD', KEYS[1], '*', 'event', ARGV[2]) end
return redis.call('XREADGROUP', 'GROUP', ARGV[3], 'abandoned', 'COUNT', ARGV[1], 'STREAMS', KEYS[1], '>')
`, []string{w.stream}, count, raw, w.group).Err(); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestReclaimPaginatesAndContinuesCursor(t *testing.T) {
	w, client := recoveryWorker(t)
	abandon(t, w, client, 1105, notification(t))
	if err := w.reclaimPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err := client.XPending(context.Background(), w.stream, w.group).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 105 || w.claimCursor == "0-0" {
		t.Fatalf("first pass: pending=%d cursor=%s", pending.Count, w.claimCursor)
	}
	if err := w.reclaimPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err = client.XPending(context.Background(), w.stream, w.group).Result()
	if err != nil || pending.Count != 0 || w.claimCursor != "0-0" {
		t.Fatalf("second pass: %+v, %v cursor=%s", pending, err, w.claimCursor)
	}
}

func TestRunReclaimsMessagesAbandonedAfterStartup(t *testing.T) {
	w, client := recoveryWorker(t)
	if w.Ready(context.Background()) == nil {
		t.Fatal("ready before startup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("shutdown stalled")
		}
	})
	eventually(t, func() bool { return w.Ready(ctx) == nil })
	abandon(t, w, client, 3, notification(t))
	eventually(t, func() bool {
		p, err := client.XPending(ctx, w.stream, w.group).Result()
		return err == nil && p.Count == 0
	})
}

func TestMalformedEventsAreDurablyDeadLettered(t *testing.T) {
	for _, raw := range []string{"not-json", `{}`, `{"id":"bad","type":"unknown","data":{}}`, `{"id":"bad","type":"order_canceled","data":{"orderId":"not-a-uuid"}}`} {
		t.Run(raw, func(t *testing.T) {
			w, client := recoveryWorker(t)
			abandon(t, w, client, 1, raw)
			if err := w.reclaimPending(context.Background()); err != nil {
				t.Fatal(err)
			}
			p, _ := client.XPending(context.Background(), w.stream, w.group).Result()
			dead, err := client.XRange(context.Background(), w.stream+":dead-letter", "-", "+").Result()
			if err != nil || p.Count != 0 || len(dead) != 1 {
				t.Fatalf("pending=%+v dead=%+v err=%v", p, dead, err)
			}
			if !strings.Contains(dead[0].Values["entry"].(string), "event") || dead[0].Values["source_id"] == "" {
				t.Fatal("original entry was not preserved")
			}
		})
	}
}

func TestFailedDeadLetterWriteLeavesEventPending(t *testing.T) {
	w, client := recoveryWorker(t)
	abandon(t, w, client, 1, "not-json")
	client.Set(context.Background(), w.stream+":dead-letter", "wrong-type", 0)
	if err := w.reclaimPending(context.Background()); err == nil {
		t.Fatal("expected DLQ failure")
	}
	p, _ := client.XPending(context.Background(), w.stream, w.group).Result()
	if p.Count != 1 {
		t.Fatal("lost malformed entry")
	}
}

func TestOperationalFailureIsBoundedAndMarksConsumerUnready(t *testing.T) {
	w, client := recoveryWorker(t)
	abandon(t, w, client, 1, notification(t))
	client.Set(context.Background(), partition.FencingKey(0), "invalid-number", 0)
	err := w.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "3 attempts") {
		t.Fatalf("expected bounded retry failure, got %v", err)
	}
	if w.Ready(context.Background()) == nil {
		t.Fatal("failed consumer still ready")
	}
	p, _ := client.XPending(context.Background(), w.stream, w.group).Result()
	if p.Count != 1 {
		t.Fatal("operational failure was acknowledged")
	}
	if n := client.XLen(context.Background(), w.stream+":dead-letter").Val(); n != 0 {
		t.Fatal("operational failure was dead-lettered")
	}
}

func TestReadyRejectsStalledConsumer(t *testing.T) {
	w, _ := recoveryWorker(t)
	w.mu.Lock()
	w.running = true
	w.progress = time.Now().Add(-31 * time.Second)
	w.mu.Unlock()
	if w.Ready(context.Background()) == nil {
		t.Fatal("stalled worker is ready")
	}
}

type temporaryFencingFailure struct{ attempts int }

func (h *temporaryFencingFailure) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *temporaryFencingFailure) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *temporaryFencingFailure) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" {
			h.attempts++
			if h.attempts <= 2 {
				return errors.New("temporary fencing read failure")
			}
		}
		return next(ctx, cmd)
	}
}

func TestTransientProcessingFailureRecoversWithinRetryBudget(t *testing.T) {
	w, client := recoveryWorker(t)
	abandon(t, w, client, 1, notification(t))
	hook := &temporaryFencingFailure{}
	client.AddHook(hook)
	if err := w.reclaimPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := client.XPending(context.Background(), w.stream, w.group).Result()
	if err != nil || p.Count != 0 || hook.attempts != 3 {
		t.Fatalf("attempts=%d pending=%+v err=%v", hook.attempts, p, err)
	}
}
