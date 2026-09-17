package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"predix/internal/events"
	"predix/internal/partition"
	rpkg "predix/pkg/redis"

	"github.com/alicebob/miniredis/v2"
)

func startTestRedisForEngine(t *testing.T) (*miniredis.Miniredis, *rpkg.RedisManager) {
	t.Helper()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}

	t.Cleanup(srv.Close)

	rm := rpkg.NewRedisManager(srv.Addr(), "")
	t.Cleanup(func() { rm.Close() })

	return srv, rm
}

// newTestEngine builds an engine with a temp outbox and registers cleanup so
// the open outbox handle is released before TempDir removal on Windows.
func newTestEngine(t *testing.T, rm *rpkg.RedisManager) *Engine {
	t.Helper()
	eng, err := newEngine(rm, filepath.Join(t.TempDir(), "outbox.log"))
	if err != nil {
		t.Fatalf("newEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.outbox.Close() })
	return eng
}

// waitFor polls cond every 50 ms until it returns true, failing after timeout.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitFor condition not met after %s", timeout)
}

func TestEngineMultiPartitionIsolation(t *testing.T) {
	_, rm := startTestRedisForEngine(t)

	eng := newTestEngine(t, rm)

	r := partition.NewRouter(2, "commands")
	eng.SetRouter(r)
	rm.SetRouter(r)

	eng.SetLeaseSettings("test-engine-1", 2*time.Second, 500*time.Millisecond)

	if err := eng.AddPartition(0); err != nil {
		t.Fatal(err)
	}

	if err := eng.AddPartition(1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eng.Start()
	defer eng.Shutdown(ctx)

	// Wait until both partitions acquired a lease and stamped a fencing token.
	waitFor(t, 5*time.Second, func() bool {
		eng.mu.RLock()
		defer eng.mu.RUnlock()
		return eng.partitionToken[0] > 0 && eng.partitionToken[1] > 0
	})

	// Fund users via fanout.
	createUserCmd := func(userID string) []byte {
		raw, _ := json.Marshal(map[string]string{"userId": userID})
		return raw
	}

	if err := rm.SendCommandFanout(ctx, rpkg.UserCreatedCommand, createUserCmd("u1")); err != nil {
		t.Fatal(err)
	}

	if err := rm.SendCommandFanout(ctx, rpkg.UserCreatedCommand, createUserCmd("u2")); err != nil {
		t.Fatal(err)
	}

	// Find two event IDs that land on different partitions.
	var e0, e1 string
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("evt-%d", i)

		if r.Partition(candidate) == 0 {
			e0 = candidate
		}

		if r.Partition(candidate) == 1 {
			e1 = candidate
		}

		if e0 != "" && e1 != "" {
			break
		}
	}

	if e0 == "" || e1 == "" {
		t.Fatal("could not find events for both partitions")
	}

	// Create events.
	createEventPayload := func(eventID string) []byte {
		raw, _ := json.Marshal(map[string]string{"eventId": eventID})
		return raw
	}

	resp, err := rm.SendAndAwait(ctx, e0, rpkg.MessageToEngine{
		Type:    string(rpkg.CreateEventCommand),
		Payload: createEventPayload(e0),
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Fatalf("create event p0: %s", resp.Error)
	}

	resp, err = rm.SendAndAwait(ctx, e1, rpkg.MessageToEngine{
		Type:    string(rpkg.CreateEventCommand),
		Payload: createEventPayload(e1),
	})

	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Fatalf("create event p1: %s", resp.Error)
	}

	// Place two orders on each event to generate 2 ORDER_CREATED per partition.
	placeBuy := func(eventID string, orderID string) {
		t.Helper()

		raw, _ := json.Marshal(map[string]any{
			"orderId":   orderID,
			"eventId":   eventID,
			"userId":    "u1",
			"orderType": "LIMIT",
			"outcome":   "YES",
			"side":      "BUY",
			"quantity":  5,
			"price":     6000,
		})

		resp, err := rm.SendAndAwait(ctx, eventID, rpkg.MessageToEngine{
			Type:    string(rpkg.CreateOrderCommand),
			Payload: raw,
		})

		if err != nil {
			t.Fatal(err)
		}

		if !resp.Success {
			t.Fatalf("buy order on %s: %s", eventID, resp.Error)
		}
	}

	placeBuy(e0, "buy-p0-1")
	placeBuy(e0, "buy-p0-2")
	placeBuy(e1, "buy-p1-1")
	placeBuy(e1, "buy-p1-2")

	// Read all envelopes from the events:out stream.
	client := rm.GetClient()

	streams, err := client.XRange(ctx, events.EventStream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}

	type envelopeInfo struct {
		PartitionID  int
		Sequence     uint64
		FencingToken uint64
		Type         events.EventType
		EventID      string
	}

	var envelopes []envelopeInfo

	for _, streamEntry := range streams {
		raw, _ := streamEntry.Values["event"].(string)
		if raw == "" {
			continue
		}

		var env events.EventEnvelope

		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}

		info := envelopeInfo{
			PartitionID:  env.PartitionID,
			Sequence:     env.Sequence,
			FencingToken: env.FencingToken,
			Type:         env.Type,
		}

		// Extract eventId from order data.
		if env.Type == events.EventOrderCreated {
			var data struct {
				EventID string `json:"eventId"`
			}

			_ = json.Unmarshal(env.Data, &data)
			info.EventID = data.EventID
		}

		envelopes = append(envelopes, info)
	}

	if len(envelopes) < 4 {
		t.Fatalf("expected at least 4 ORDER_CREATED envelopes, got %d", len(envelopes))
	}

	// ---- Assertions ----

	// 1. Every envelope has a positive fencing token.
	for _, env := range envelopes {
		if env.FencingToken == 0 {
			t.Errorf("envelope partition=%d seq=%d has zero fencing token",
				env.PartitionID, env.Sequence)
		}
	}

	// 2. Per-partition sequences are strictly increasing starting at 1,
	//    and partition ID matches the event's router partition.
	partitionSeqs := map[int][]uint64{}

	for _, env := range envelopes {
		if env.Type != events.EventOrderCreated {
			continue
		}

		expected := r.Partition(env.EventID)
		if env.PartitionID != expected {
			t.Errorf("envelope partition=%d does not match router partition=%d for %s",
				env.PartitionID, expected, env.EventID)
		}

		partitionSeqs[env.PartitionID] = append(partitionSeqs[env.PartitionID], env.Sequence)
	}

	for p := 0; p < 2; p++ {
		seqs := partitionSeqs[p]
		if len(seqs) < 2 {
			t.Errorf("partition %d: expected at least 2 envelopes, got %d", p, len(seqs))
			continue
		}

		for i, seq := range seqs {
			expected := uint64(i + 1)
			if seq != expected {
				t.Errorf("partition %d: envelope %d has sequence %d, want %d",
					p, i, seq, expected)
			}
		}

		// Verify monotonic.
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Errorf("partition %d: sequence %d <= %d (not monotonic)",
					p, seqs[i], seqs[i-1])
			}
		}
	}

	// 3. User balance confirms USER_CREATED was applied exactly once (no double-fund).
	avail, reserved, ok := eng.GetBalances("u1")
	if !ok {
		t.Fatal("u1 balance not found after USER_CREATED fanout")
	}

	// 4 orders * 5 qty * 0.6000 price = 12.00 reserved.
	expectedReserved := int64(4 * 5 * 6000)
	if reserved != expectedReserved {
		t.Errorf("u1 reserved = %d, want %d", reserved, expectedReserved)
	}

	expectedAvail := DefaultStartingBalance - expectedReserved
	if avail != expectedAvail {
		t.Errorf("u1 available = %d, want %d", avail, expectedAvail)
	}
}

// TestEngineAddPartitionValidation checks boundary conditions on AddPartition.
func TestEngineAddPartitionValidation(t *testing.T) {
	rm := rpkg.NewRedisManager("localhost:0", "")
	defer rm.Close()

	eng := newTestEngine(t, rm)

	r := partition.NewRouter(4, "commands")
	eng.SetRouter(r)

	// Valid partition.
	if err := eng.AddPartition(0); err != nil {
		t.Errorf("AddPartition(0) = %v", err)
	}

	// Duplicate.
	if err := eng.AddPartition(0); err == nil {
		t.Error("AddPartition(0) should reject duplicate")
	}

	// Out of range.
	if err := eng.AddPartition(4); err == nil {
		t.Error("AddPartition(4) should reject out-of-range")
	}

	if err := eng.AddPartition(-1); err == nil {
		t.Error("AddPartition(-1) should reject negative")
	}
}

// TestEngineEmitEventStamping verifies that the envelope stamped by
// emitEventP carries the correct fencing token and sequence from the engine's
// per-partition counters.
func TestEngineEmitEventStamping(t *testing.T) {
	rm := rpkg.NewRedisManager("localhost:0", "")
	defer rm.Close()

	eng := newTestEngine(t, rm)

	r := partition.NewRouter(2, "commands")
	eng.SetRouter(r)

	// Seed partition token and sequence counters.
	eng.mu.Lock()
	eng.partitionToken[0] = 42
	eng.partitionToken[1] = 99
	eng.mu.Unlock()

	eng.partitionSeq[0].Add(5) // seq for p0 will start at 6
	eng.partitionSeq[1].Add(3) // seq for p1 will start at 4

	data, _ := json.Marshal(map[string]string{"key": "value"})

	// emitEventP uses redisManager.GetClient() for XAdd, so we need a real
	// miniredis.
	srv, err2 := miniredis.Run()
	if err2 != nil {
		t.Fatal(err2)
	}

	defer srv.Close()

	rm2 := rpkg.NewRedisManager(srv.Addr(), "")
	defer rm2.Close()

	eng2, _ := NewEngine(rm2)
	eng2.SetRouter(partition.NewRouter(2, "commands"))

	eng2.mu.Lock()
	eng2.partitionToken[0] = 42
	eng2.partitionToken[1] = 99
	eng2.mu.Unlock()

	eng2.partitionSeq[0].Add(5)
	eng2.partitionSeq[1].Add(3)

	// Emit from partition 0.
	if err := eng2.emitEventP(0, events.EventOrderCreated, data, false); err != nil {
		t.Fatal(err)
	}

	// Emit from partition 1.
	if err := eng2.emitEventP(1, events.EventOrderCreated, data, false); err != nil {
		t.Fatal(err)
	}

	// Read envelopes.
	client := rm2.GetClient()
	streams, err := client.XRange(context.Background(), events.EventStream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}

	if len(streams) != 2 {
		t.Fatalf("expected 2 envelopes, got %d", len(streams))
	}

	// Parse envelopes.
	var envelopes []events.EventEnvelope

	for _, entry := range streams {
		raw, _ := entry.Values["event"].(string)

		var env events.EventEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatal(err)
		}

		envelopes = append(envelopes, env)
	}

	// Envelope 0: partition 0, seq 6, token 42.
	if envelopes[0].PartitionID != 0 {
		t.Errorf("env0 partition = %d, want 0", envelopes[0].PartitionID)
	}

	if envelopes[0].Sequence != 6 {
		t.Errorf("env0 sequence = %d, want 6", envelopes[0].Sequence)
	}

	if envelopes[0].FencingToken != 42 {
		t.Errorf("env0 fencingToken = %d, want 42", envelopes[0].FencingToken)
	}

	// Envelope 1: partition 1, seq 4, token 99.
	if envelopes[1].PartitionID != 1 {
		t.Errorf("env1 partition = %d, want 1", envelopes[1].PartitionID)
	}

	if envelopes[1].Sequence != 4 {
		t.Errorf("env1 sequence = %d, want 4", envelopes[1].Sequence)
	}

	if envelopes[1].FencingToken != 99 {
		t.Errorf("env1 fencingToken = %d, want 99", envelopes[1].FencingToken)
	}
}

// TestEngineEmitEventSuppressionDuringReplay verifies that emitEventP is a
// no-op when the replay gate is set for the partition.
func TestEngineEmitEventSuppressionDuringReplay(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}

	defer srv.Close()

	rm := rpkg.NewRedisManager(srv.Addr(), "")
	defer rm.Close()

	eng := newTestEngine(t, rm)

	eng.SetRouter(partition.NewRouter(1, "commands"))

	eng.mu.Lock()
	eng.replaying[0] = true
	eng.mu.Unlock()

	data, _ := json.Marshal(map[string]string{"key": "value"})

	if err := eng.emitEventP(0, events.EventOrderCreated, data, false); err != nil {
		t.Fatal(err)
	}

	client := rm.GetClient()
	count, _ := client.XLen(context.Background(), events.EventStream).Result()

	if count != 0 {
		t.Errorf("expected 0 envelopes during replay, got %d", count)
	}
}

// TestEngineEmitMuSerializesConcurrentPartitions ensures that concurrent
// partition workers cannot interleave the outbox append/XAdd/reset cycle.
func TestEngineEmitMuSerializesConcurrentPartitions(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}

	defer srv.Close()

	rm := rpkg.NewRedisManager(srv.Addr(), "")
	defer rm.Close()

	eng := newTestEngine(t, rm)

	eng.SetRouter(partition.NewRouter(2, "commands"))

	eng.mu.Lock()
	eng.partitionToken[0] = 1
	eng.partitionToken[1] = 2
	eng.mu.Unlock()

	data, _ := json.Marshal(map[string]string{"key": "value"})

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(partitionID int) {
			defer wg.Done()
			_ = eng.emitEventP(partitionID, events.EventOrderCreated, data, false)
		}(i % 2)
	}

	wg.Wait()

	client := rm.GetClient()
	count, _ := client.XLen(context.Background(), events.EventStream).Result()

	if count != 100 {
		t.Errorf("expected 100 envelopes, got %d", count)
	}

	// Per-partition sequences should be exactly 1..50 each.
	p0Count := 0
	p1Count := 0

	streams, _ := client.XRange(context.Background(), events.EventStream, "-", "+").Result()

	for _, entry := range streams {
		raw, _ := entry.Values["event"].(string)

		var env events.EventEnvelope
		json.Unmarshal([]byte(raw), &env)

		switch env.PartitionID {
		case 0:
			p0Count++
		case 1:
			p1Count++
		}
	}

	if p0Count != 50 || p1Count != 50 {
		t.Errorf("partition distribution = (%d, %d), want (50, 50)", p0Count, p1Count)
	}
}

func TestEngineLeaseHolderLoop(t *testing.T) {
	srv, rm := startTestRedisForEngine(t)

	eng := newTestEngine(t, rm)

	r := partition.NewRouter(1, "commands")
	eng.SetRouter(r)
	rm.SetRouter(r)
	eng.SetLeaseSettings("lease-test-engine", 1*time.Second, 250*time.Millisecond)

	if err := eng.AddPartition(0); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng.Start()
	defer eng.Shutdown(ctx)

	// Wait for token acquisition.
	waitFor(t, 5*time.Second, func() bool {
		eng.mu.RLock()
		defer eng.mu.RUnlock()
		return eng.partitionToken[0] > 0
	})

	// Lease key exists in Redis.
	client := rm.GetClient()
	val, err := client.Get(ctx, "engine:lease:0").Result()
	if err != nil {
		t.Fatalf("lease key missing: %v", err)
	}

	if val != "lease-test-engine" {
		t.Errorf("lease owner = %s, want lease-test-engine", val)
	}

	// Fast-forward past TTL: lease should expire and be re-acquired.
	srv.FastForward(2 * time.Second)

	waitFor(t, 3*time.Second, func() bool {
		val, err := client.Get(ctx, "engine:lease:0").Result()
		return err == nil && val == "lease-test-engine"
	})

	// Fencing token should have incremented.
	eng.mu.RLock()
	token := eng.partitionToken[0]
	eng.mu.RUnlock()

	if token < 2 {
		t.Errorf("fencing token = %d, want >= 2 after lease re-acquire", token)
	}
}

// TestEngineFailoverTransfersOwnership verifies P2.6: after engine A stops,
// engine B acquires the released partitions and rebuilds state under a newer
// fencing token.
func TestEngineFailoverTransfersOwnership(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}

	defer srv.Close()

	rm1 := rpkg.NewRedisManager(srv.Addr(), "")
	defer rm1.Close()

	rm2 := rpkg.NewRedisManager(srv.Addr(), "")
	defer rm2.Close()

	eng1, err := newEngine(rm1, filepath.Join(t.TempDir(), "outbox1.log"))
	if err != nil {
		t.Fatal(err)
	}

	eng2 := newTestEngine(t, rm2)

	r := partition.NewRouter(2, "commands")

	eng1.SetRouter(r)
	rm1.SetRouter(r)

	eng2.SetRouter(r)
	rm2.SetRouter(r)

	eng1.SetLeaseSettings("engine-1", 1*time.Second, 250*time.Millisecond)
	eng2.SetLeaseSettings("engine-2", 1*time.Second, 250*time.Millisecond)

	for pid := 0; pid < r.Partitions; pid++ {
		if err := eng1.AddPartition(pid); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng1.Start()

	// Engine 1 acquires both partitions.
	waitFor(t, 5*time.Second, func() bool {
		eng1.mu.RLock()
		defer eng1.mu.RUnlock()
		return eng1.partitionToken[0] > 0 && eng1.partitionToken[1] > 0
	})

	token1p0 := partitionToken(t, eng1, 0)
	token1p1 := partitionToken(t, eng1, 1)

	// Engine 1 stops, releasing its leases.
	eng1.Shutdown(ctx)

	// Engine 2 takes over both partitions.
	for pid := 0; pid < r.Partitions; pid++ {
		if err := eng2.AddPartition(pid); err != nil {
			t.Fatal(err)
		}
	}

	eng2.Start()
	defer eng2.Shutdown(ctx)

	waitFor(t, 5*time.Second, func() bool {
		eng2.mu.RLock()
		defer eng2.mu.RUnlock()
		return eng2.partitionToken[0] > 0 && eng2.partitionToken[1] > 0
	})

	// Lease keys now belong to engine-2.
	client := rm2.GetClient()

	for pid := 0; pid < r.Partitions; pid++ {
		owner, err := client.Get(ctx, r.LeaseKey(pid)).Result()
		if err != nil {
			t.Fatalf("partition %d lease missing after failover: %v", pid, err)
		}

		if owner != "engine-2" {
			t.Errorf("partition %d owner = %s, want engine-2", pid, owner)
		}

		// The successor holds a strictly newer fencing token (P2.5).
		token2 := partitionToken(t, eng2, pid)

		var oldToken uint64
		if pid == 0 {
			oldToken = token1p0
		} else {
			oldToken = token1p1
		}

		if token2 <= oldToken {
			t.Errorf("partition %d fencing token %d not newer than predecessor %d",
				pid, token2, oldToken)
		}
	}
}

func partitionToken(t *testing.T, e *Engine, pid int) uint64 {
	t.Helper()
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.partitionToken[pid]
}
