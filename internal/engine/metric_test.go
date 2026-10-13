package engine

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"predix/internal/events"
	pkgredis "predix/pkg/redis"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// metricValue finds a metric family by name and returns its first value, or
// nil when the family is absent.
func metricValue(reg *prometheus.Registry, name string) float64 {
	families, err := reg.Gather()
	if err != nil {
		return 0
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

// metricCount returns the sample count of a _(histogram|summary)_ histogram.
func metricCount(reg *prometheus.Registry, name string) uint64 {
	families, err := reg.Gather()
	if err != nil {
		return 0
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func TestEmitTradeInstrumentsRedisLatencyAndSettlement(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rm := pkgredis.NewRedisManager(srv.Addr(), "")
	eng, err := NewEngineWithOutbox(
		rm,
		filepath.Join(t.TempDir(), "outbox.log"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.outbox.Close() })

	eng.SetStreamNames("events:out", "ws:updates")
	eng.partitionToken = map[int]uint64{0: 1}

	data, _ := json.Marshal(map[string]interface{}{
		"orderId": "o1",
		"price":   int64(5000),
		"amount":  int64(10),
	})

	if err := eng.emitEventP(0, events.EventTradeExecuted, data, true); err != nil {
		t.Fatalf("happy-path emit failed: %v", err)
	}

	// XAdd (durable stream) + Publish (WS fan-out) were both observed.
	if got := metricCount(eng.metrics.Registry, "engine_redis_latency_seconds"); got != 2 {
		t.Errorf("redis latency observations = %d, want 2", got)
	}
	if got := metricValue(eng.metrics.Registry, "engine_settlement_failures_total"); got != 0 {
		t.Errorf("settlement failures = %v, want 0", got)
	}

	// Take Redis down: the next trade publish fails and is counted.
	srv.Close()
	if err := eng.emitEventP(0, events.EventTradeExecuted, data, true); err == nil {
		t.Fatal("expected emit to fail with Redis down")
	}
	if got := metricValue(eng.metrics.Registry, "engine_settlement_failures_total"); got != 1 {
		t.Errorf("settlement failures = %v, want 1", got)
	}
	// Only successful writes are timed; the failed XAdd is not.
	if got := metricCount(eng.metrics.Registry, "engine_redis_latency_seconds"); got != 2 {
		t.Errorf("redis latency observations = %d, want 2", got)
	}
	if got := eng.metrics.RedisLatency != nil; !got {
		t.Error("RedisLatency histogram missing")
	}
}
