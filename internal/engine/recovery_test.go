package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rd "github.com/redis/go-redis/v9"
	"predix/internal/partition"
	rpkg "predix/pkg/redis"
)

func TestReplayRejectsMalformedLogBeforeApplyingCommands(t *testing.T) {
	for _, raw := range []string{"", "invalid-json", `{}`, `{"type":"UNKNOWN","payload":{}}`, `{"type":"CREATE_EVENT","payload":null}`, `{"type":"CREATE_EVENT","payload":{"eventId":123}}`, `{"type":"CREATE_EVENT","payload":{"eventId":"evt","expiresAt":"invalid-date"}}`, `{"type":"CREATE_ORDER","payload":{"quantity":1}}`} {
		t.Run(raw, func(t *testing.T) {
			_, rm := startTestRedisForEngine(t)
			e := newTestEngine(t, rm)
			valid, _ := json.Marshal(envelope(t, rpkg.CreateEventCommand, map[string]string{"eventId": "valid-event"}))
			client := rm.GetClient()
			client.XAdd(context.Background(), &rd.XAddArgs{Stream: e.router.Stream(0), Values: map[string]interface{}{"command": string(valid)}})
			client.XAdd(context.Background(), &rd.XAddArgs{Stream: e.router.Stream(0), Values: map[string]interface{}{"command": raw}})
			if err := e.replayCommandLog(context.Background(), 0); err == nil {
				t.Fatal("corrupt log was accepted")
			}
			if len(e.markets) != 0 {
				t.Fatal("partial log was applied")
			}
			if e.isReplaying(0) {
				t.Fatal("replay suppression leaked")
			}
		})
	}
}

func TestReplayFailureReleasesLeaseAndBlocksConsumptionUntilRecovery(t *testing.T) {
	_, rm := startTestRedisForEngine(t)
	e := newTestEngine(t, rm)
	router := partition.NewRouter(1, "commands")
	e.SetRouter(router)
	rm.SetRouter(router)
	e.SetLeaseSettings("recovery-test", time.Second, 50*time.Millisecond)
	if err := e.AddPartition(0); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	client := rm.GetClient()
	bad, err := client.XAdd(ctx, &rd.XAddArgs{Stream: router.Stream(0), Values: map[string]interface{}{"command": "invalid-json"}}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rm.SendCommand(ctx, "blocked-event", rpkg.CreateEventCommand, mustJSONTest(t, map[string]string{"eventId": "blocked-event"})); err != nil {
		t.Fatal(err)
	}
	e.Start()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		e.Shutdown(shutdownCtx)
	}()
	waitFor(t, 3*time.Second, func() bool {
		token, err := client.Get(ctx, router.FencingKey(0)).Int64()
		return err == nil && token >= 2 && client.Exists(ctx, router.LeaseKey(0)).Val() == 0
	})
	if e.OwnedPartitions() != 0 {
		t.Fatal("failed recovery advertised readiness")
	}
	groups, err := client.XInfoGroups(ctx, router.Stream(0)).Result()
	if err != nil || len(groups) != 0 {
		t.Fatalf("consumption started after replay failure: %v %+v", err, groups)
	}
	e.mu.RLock()
	markets := len(e.markets)
	e.mu.RUnlock()
	if markets != 0 {
		t.Fatal("commands applied despite corrupt log")
	}
	if err := client.XDel(ctx, router.Stream(0), bad).Err(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return e.OwnedPartitions() == 1 })
	e.mu.RLock()
	_, recovered := e.markets["blocked-event"]
	e.mu.RUnlock()
	if !recovered {
		t.Fatal("partition ready without reconstructed state")
	}
	if err := rm.SendCommand(ctx, "new-event", rpkg.CreateEventCommand, mustJSONTest(t, map[string]string{"eventId": "new-event"})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { e.mu.RLock(); defer e.mu.RUnlock(); return e.markets["new-event"] != nil })
}

func TestCanceledReplayNeverAppliesState(t *testing.T) {
	_, rm := startTestRedisForEngine(t)
	e := newTestEngine(t, rm)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.replayCommandLog(ctx, 0); err == nil {
		t.Fatal("canceled replay succeeded")
	}
	if len(e.markets) != 0 || e.isReplaying(0) {
		t.Fatal("canceled replay mutated state")
	}
}
